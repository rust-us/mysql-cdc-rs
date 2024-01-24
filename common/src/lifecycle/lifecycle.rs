
pub trait Lifecycle {
    /// 初始化
    fn setup();

    /// 启动
    fn start();

    /// 关闭
    fn stop();

    /// 暂停服务，服务挂起
    fn pause();


}