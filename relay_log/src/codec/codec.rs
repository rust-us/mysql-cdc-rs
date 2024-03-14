
/// 编解码
pub trait Codec {
    /// 实例化
    fn new() -> Self where Self: Sized;

    /// 实例类型名称
    fn name(&self) -> String;

}