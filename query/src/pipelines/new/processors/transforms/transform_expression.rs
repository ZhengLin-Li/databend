use std::sync::Arc;
use common_datablocks::DataBlock;
use common_datavalues2::DataSchemaRef;
use common_planners::Expression;
use crate::pipelines::transforms::ExpressionExecutor;
use common_exception::Result;
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::transform::{Transform, Transformer};

pub type ProjectionTransform = ExpressionTransformImpl<true>;
pub type ExpressionTransform = ExpressionTransformImpl<false>;

pub struct ExpressionTransformImpl<const ALIAS_PROJECT: bool> {
    executor: ExpressionExecutor,
}

impl<const ALIAS_PROJECT: bool> ExpressionTransformImpl<ALIAS_PROJECT> where Self: Transform {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        exprs: Vec<Expression>) -> Result<ProcessorPtr> {
        let executor = ExpressionExecutor::try_create(
            "expression executor",
            input_schema,
            output_schema,
            exprs,
            ALIAS_PROJECT,
        )?;
        executor.validate()?;

        Ok(Transformer::create(input, output, Self { executor }))
    }
}

impl Transform for ExpressionTransformImpl<true> {
    const NAME: &'static str = "ProjectionTransform";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        self.executor.execute(&data)
    }
}

impl Transform for ExpressionTransformImpl<false> {
    const NAME: &'static str = "ExpressionTransform";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        self.executor.execute(&data)
    }
}

