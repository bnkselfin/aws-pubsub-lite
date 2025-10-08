#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MessageDeleteMode {
    LeaveIt,
    DeleteAllCalled,
    DeleteAllHandled,
}
