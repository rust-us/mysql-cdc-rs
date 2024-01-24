
#[cfg(test)]
mod test {
    use tracing::debug;
    use common::log::tracing_factory::TracingFactory;

    #[test]
    fn test() {
        TracingFactory::init_log(true);

        debug!("test");
    }
}