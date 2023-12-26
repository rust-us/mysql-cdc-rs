
#[cfg(test)]
mod test {
    use common::log::log_factory::LogFactory;

    #[test]
    fn test() {
        LogFactory::init_log(true);

        println!("test");
    }
}