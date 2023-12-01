
#[test]
fn test_stop() {
    let mut input = include_bytes!("../events/03_stop/log.bin");
    println!("{:?}", input);

    let (remain, output) = Event::from_bytes(input).unwrap();
    assert_eq!(remain.len(), 0);
    match output.get(2).unwrap() {
        Stop { .. } => {}
        _ => panic!("should be stop event"),
    }
}