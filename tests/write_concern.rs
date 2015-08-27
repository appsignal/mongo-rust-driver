use mongo_driver::write_concern::WriteConcern;

#[test]
fn test_write_concern() {
    let write_concern = WriteConcern::new();
    assert!(!write_concern.inner().is_null());
}
