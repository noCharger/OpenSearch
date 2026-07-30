/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! UNNEST extension for consuming Substrait ExtensionSingleRel.
//!
//! This module wires the Substrait→DataFusion extension path for lateral UNNEST.
//! The Java DataFusionFragmentConvertor emits:
//!
//! ```text
//! ExtensionSingleRel {
//!     input: <child rel>,
//!     detail: Any {
//!         type_url: "/opensearch.analytics.unnest.v1",
//!         value: UTF8 JSON = {"arrayColIdx": N, "elementTypeName": "Str", "ordinality": false}
//!     }
//! }
//! ```
//!
//! The `arrayColIdx` is the array column index in the INPUT (child) schema.
//! The declared output schema = input columns + ONE appended element column (Option 1 append layout).
//!
//! DataFusion's native `UnnestExec` replaces the list column IN PLACE. To achieve the APPEND
//! layout, we:
//! 1. Build a projection that duplicates the array column at `arrayColIdx` as a new trailing column
//! 2. Unnest that trailing copy → output = [..input cols.., element]
//!
//! ## Components
//!
//! - [`UnnestExtensionNode`]: A `UserDefinedLogicalNode` carrying the unnest parameters
//! - [`OpenSearchSerializerRegistry`]: Deserializes the ExtensionSingleRel detail bytes into `UnnestExtensionNode`
//! - [`UnnestExtensionPlanner`]: Lowers `UnnestExtensionNode` to physical plan using native `UnnestExec`

use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::common::{DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion::common::UnnestOptions;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::unnest::{ListUnnest, UnnestExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_expr::registry::SerializerRegistry;

/// Type URL for the UNNEST extension in Substrait.
pub const UNNEST_TYPE_URL: &str = "/opensearch.analytics.unnest.v1";

/// JSON payload structure for the UNNEST extension.
#[derive(Debug, Clone)]
pub struct UnnestDetail {
    pub array_col_idx: usize,
    pub element_type_name: String,
    pub ordinality: bool,
}

impl UnnestDetail {
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| {
            DataFusionError::Plan(format!(
                "Failed to parse UnnestDetail JSON: {}. Bytes: {:?}",
                e,
                String::from_utf8_lossy(bytes)
            ))
        })?;
        
        let array_col_idx = value.get("arrayColIdx")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| DataFusionError::Plan("Missing or invalid arrayColIdx".to_string()))?
            as usize;
        
        let element_type_name = value.get("elementTypeName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| DataFusionError::Plan("Missing or invalid elementTypeName".to_string()))?
            .to_string();
        
        let ordinality = value.get("ordinality")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        
        Ok(Self {
            array_col_idx,
            element_type_name,
            ordinality,
        })
    }
}

/// A UserDefinedLogicalNode representing the UNNEST operation.
///
/// This node wraps a single input and carries the parameters needed to
/// construct a physical UnnestExec. The output schema is the input schema
/// with ONE appended element column.
///
/// When first deserialized (no input yet), `input` is None and `schema` is empty.
/// The Substrait consumer then calls `with_exprs_and_inputs` to attach the real input,
/// at which point we compute the proper output schema.
#[derive(Debug, Clone)]
pub struct UnnestExtensionNode {
    input: Option<LogicalPlan>,
    array_col_idx: usize,
    element_type_name: String,
    ordinality: bool,
    schema: DFSchemaRef,
}

impl UnnestExtensionNode {
    /// Create a new UnnestExtensionNode without an input (placeholder state).
    ///
    /// This is called by the SerializerRegistry when deserializing. The input will be
    /// attached later via `with_exprs_and_inputs`.
    pub fn new_placeholder(
        array_col_idx: usize,
        element_type_name: String,
        ordinality: bool,
    ) -> Self {
        Self {
            input: None,
            array_col_idx,
            element_type_name,
            ordinality,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    /// Create a new UnnestExtensionNode with the given input.
    ///
    /// Builds the output schema by appending the element column to the input schema.
    pub fn new_with_input(
        input: LogicalPlan,
        array_col_idx: usize,
        element_type_name: String,
        ordinality: bool,
    ) -> Result<Self> {
        let input_schema = input.schema();
        
        if array_col_idx >= input_schema.fields().len() {
            return Err(DataFusionError::Plan(format!(
                "UnnestExtensionNode: arrayColIdx {} out of bounds for input schema with {} fields",
                array_col_idx,
                input_schema.fields().len()
            )));
        }

        let array_field = input_schema.field(array_col_idx);
        let element_type = match array_field.data_type() {
            DataType::List(inner) => inner.data_type().clone(),
            DataType::LargeList(inner) => inner.data_type().clone(),
            DataType::FixedSizeList(inner, _) => inner.data_type().clone(),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "UnnestExtensionNode: column {} has type {:?}, expected List/LargeList/FixedSizeList",
                    array_col_idx, other
                )));
            }
        };

        // Output schema = input columns + one appended element column (Option 1 append layout).
        let mut arrow_fields: Vec<Arc<Field>> = input_schema
            .fields()
            .iter()
            .map(|f| Arc::clone(f))
            .collect();

        let element_col_name = format!("{}_element", array_field.name());
        let element_field = Arc::new(Field::new(
            &element_col_name,
            element_type,
            true,
        ));
        arrow_fields.push(element_field);

        let arrow_schema = Schema::new_with_metadata(
            arrow_fields.into_iter().map(|f| (*f).clone()).collect::<Vec<Field>>(),
            input_schema.metadata().clone(),
        );
        let output_schema = DFSchema::try_from(arrow_schema)?;

        Ok(Self {
            input: Some(input),
            array_col_idx,
            element_type_name,
            ordinality,
            schema: Arc::new(output_schema),
        })
    }

    pub fn array_col_idx(&self) -> usize {
        self.array_col_idx
    }

    pub fn ordinality(&self) -> bool {
        self.ordinality
    }

    pub fn input(&self) -> Option<&LogicalPlan> {
        self.input.as_ref()
    }
}

impl PartialEq for UnnestExtensionNode {
    fn eq(&self, other: &Self) -> bool {
        self.array_col_idx == other.array_col_idx
            && self.element_type_name == other.element_type_name
            && self.ordinality == other.ordinality
            && self.input == other.input
    }
}

impl Eq for UnnestExtensionNode {}

impl PartialOrd for UnnestExtensionNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.array_col_idx.partial_cmp(&other.array_col_idx) {
            Some(std::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.element_type_name.partial_cmp(&other.element_type_name) {
            Some(std::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.ordinality.partial_cmp(&other.ordinality)
    }
}

impl Hash for UnnestExtensionNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.array_col_idx.hash(state);
        self.element_type_name.hash(state);
        self.ordinality.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for UnnestExtensionNode {
    fn name(&self) -> &str {
        "UnnestExtension"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        match &self.input {
            Some(plan) => vec![plan],
            None => vec![],
        }
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "UnnestExtension: array_col_idx={}, element_type={}, ordinality={}",
            self.array_col_idx, self.element_type_name, self.ordinality
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        match inputs.len() {
            0 => Ok(self.clone()),
            1 => {
                let input = inputs.into_iter().next().unwrap();
                UnnestExtensionNode::new_with_input(
                    input,
                    self.array_col_idx,
                    self.element_type_name.clone(),
                    self.ordinality,
                )
            }
            n => Err(DataFusionError::Plan(format!(
                "UnnestExtensionNode expects 0 or 1 input, got {}",
                n
            ))),
        }
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        // The element column is generated by unnest, so predicates cannot push below it.
        let mut cols = HashSet::new();
        if let Some(field) = self.schema.fields().last() {
            cols.insert(field.name().clone());
        }
        cols
    }
}

/// Serializer registry for OpenSearch analytics extensions.
///
/// Handles deserialization of ExtensionSingleRel detail bytes into UserDefinedLogicalNode.
#[derive(Debug, Default)]
pub struct OpenSearchSerializerRegistry;

impl SerializerRegistry for OpenSearchSerializerRegistry {
    fn serialize_logical_plan(&self, node: &dyn UserDefinedLogicalNode) -> Result<Vec<u8>> {
        // We only need deserialization for consuming Substrait from Java.
        // Serialization is not needed since we don't produce Substrait back to Java.
        Err(DataFusionError::NotImplemented(format!(
            "Serialization not implemented for node: {}",
            node.name()
        )))
    }

    fn deserialize_logical_plan(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<Arc<dyn UserDefinedLogicalNode>> {
        match name {
            UNNEST_TYPE_URL => {
                let detail = UnnestDetail::from_json(bytes)?;

                // Create a placeholder node. The Substrait consumer will call
                // with_exprs_and_inputs to attach the real input.
                let node = UnnestExtensionNode::new_placeholder(
                    detail.array_col_idx,
                    detail.element_type_name,
                    detail.ordinality,
                );
                Ok(Arc::new(node))
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unknown extension type_url: {}",
                name
            ))),
        }
    }
}

/// Extension planner that lowers UnnestExtensionNode to physical UnnestExec.
#[derive(Debug, Default)]
pub struct UnnestExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for UnnestExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(unnest_node) = node.as_any().downcast_ref::<UnnestExtensionNode>() else {
            return Ok(None); // not our node; delegate to other planners
        };

        if physical_inputs.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "UnnestExtensionNode expects exactly 1 physical input, got {}",
                physical_inputs.len()
            )));
        }

        let input = Arc::clone(&physical_inputs[0]);
        let input_schema = input.schema();
        let array_col_idx = unnest_node.array_col_idx();

        if array_col_idx >= input_schema.fields().len() {
            return Err(DataFusionError::Plan(format!(
                "UnnestExtensionPlanner: array_col_idx {} out of bounds for input schema with {} fields",
                array_col_idx,
                input_schema.fields().len()
            )));
        }

        let array_field = input_schema.field(array_col_idx);
        let element_type = match array_field.data_type() {
            DataType::List(inner) => inner.data_type().clone(),
            DataType::LargeList(inner) => inner.data_type().clone(),
            DataType::FixedSizeList(inner, _) => inner.data_type().clone(),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "UnnestExtensionPlanner: column {} has type {:?}, expected List type",
                    array_col_idx, other
                )));
            }
        };

        // Strategy: To achieve APPEND layout (input cols + element at end),
        // DataFusion's UnnestExec replaces the list column in-place.
        // We need to:
        // 1. Project input to duplicate the array column at the end: [col0, col1, ..., colN, colN_copy]
        // 2. Unnest the trailing copy (colN_copy becomes element)
        // 3. Result schema: [col0, col1, ..., colN, element]

        let num_input_cols = input_schema.fields().len();

        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::projection::ProjectionExec;

        let mut proj_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> = Vec::with_capacity(num_input_cols + 1);

        for i in 0..num_input_cols {
            let field = input_schema.field(i);
            let col_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(Column::new(field.name(), i));
            proj_exprs.push((col_expr, field.name().clone()));
        }

        // Duplicate the array column at the end; UnnestExec consumes this trailing copy.
        let array_col_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(Column::new(array_field.name(), array_col_idx));
        let dup_col_name = format!("{}_unnest_temp", array_field.name());
        proj_exprs.push((array_col_expr, dup_col_name));

        let projection = Arc::new(ProjectionExec::try_new(proj_exprs, input)?);

        let mut output_fields: Vec<Arc<Field>> = Vec::with_capacity(num_input_cols + 1);
        for i in 0..num_input_cols {
            output_fields.push(Arc::new(input_schema.field(i).clone()));
        }
        let element_col_name = format!("{}_element", array_field.name());
        output_fields.push(Arc::new(Field::new(&element_col_name, element_type, true)));

        let output_schema: SchemaRef = Arc::new(Schema::new(output_fields));

        let list_unnest = ListUnnest {
            index_in_input_schema: num_input_cols,
            depth: 1,
        };

        let unnest_options = UnnestOptions {
            preserve_nulls: true,
            recursions: vec![],
        };

        let unnest_exec = UnnestExec::new(
            projection,
            vec![list_unnest],
            vec![],
            output_schema,
            unnest_options,
        )?;

        Ok(Some(Arc::new(unnest_exec)))
    }
}

/// Create the serializer registry for OpenSearch extensions.
pub fn create_serializer_registry() -> Arc<dyn SerializerRegistry> {
    Arc::new(OpenSearchSerializerRegistry)
}

/// Create the extension planner for OpenSearch extensions.
pub fn create_extension_planner() -> Arc<dyn ExtensionPlanner + Send + Sync> {
    Arc::new(UnnestExtensionPlanner)
}

/// Custom QueryPlanner that uses DefaultPhysicalPlanner with our extension planners.
pub struct OpenSearchQueryPlanner {
    extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

impl fmt::Debug for OpenSearchQueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenSearchQueryPlanner")
            .field("extension_planners_count", &self.extension_planners.len())
            .finish()
    }
}

impl Default for OpenSearchQueryPlanner {
    fn default() -> Self {
        Self {
            extension_planners: vec![Arc::new(UnnestExtensionPlanner)],
        }
    }
}

impl OpenSearchQueryPlanner {
    /// Create a new OpenSearchQueryPlanner with the given extension planners.
    pub fn new(extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>) -> Self {
        Self { extension_planners }
    }

    /// Create the default OpenSearchQueryPlanner with the UNNEST extension planner.
    pub fn with_unnest() -> Self {
        Self::default()
    }
}

use datafusion::execution::context::QueryPlanner;

#[async_trait]
impl QueryPlanner for OpenSearchQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
        
        let planner = DefaultPhysicalPlanner::with_extension_planners(
            self.extension_planners.clone()
        );
        planner.create_physical_plan(logical_plan, session_state).await
    }
}

/// Create the query planner for OpenSearch extensions.
pub fn create_query_planner() -> Arc<dyn QueryPlanner + Send + Sync> {
    Arc::new(OpenSearchQueryPlanner::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unnest_detail_from_json() {
        let json = br#"{"arrayColIdx": 2, "elementTypeName": "Str", "ordinality": false}"#;
        let detail = UnnestDetail::from_json(json).unwrap();
        assert_eq!(detail.array_col_idx, 2);
        assert_eq!(detail.element_type_name, "Str");
        assert!(!detail.ordinality);
    }

    #[test]
    fn test_unnest_detail_from_json_with_ordinality() {
        let json = br#"{"arrayColIdx": 0, "elementTypeName": "Int64", "ordinality": true}"#;
        let detail = UnnestDetail::from_json(json).unwrap();
        assert_eq!(detail.array_col_idx, 0);
        assert_eq!(detail.element_type_name, "Int64");
        assert!(detail.ordinality);
    }

    #[test]
    fn test_serializer_registry_deserialize() {
        let registry = OpenSearchSerializerRegistry;
        let json = br#"{"arrayColIdx": 1, "elementTypeName": "Str", "ordinality": false}"#;
        let node = registry.deserialize_logical_plan(UNNEST_TYPE_URL, json).unwrap();
        assert_eq!(node.name(), "UnnestExtension");
        
        // Should be a placeholder (no input yet)
        assert!(node.inputs().is_empty());
    }

    #[test]
    fn test_serializer_registry_unknown_type() {
        let registry = OpenSearchSerializerRegistry;
        let result = registry.deserialize_logical_plan("/unknown.type", b"{}");
        assert!(result.is_err());
    }
}
