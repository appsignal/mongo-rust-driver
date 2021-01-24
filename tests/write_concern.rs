extern crate mongo_driver;

use mongo_driver::write_concern::WriteConcern;

#[test]
fn test_default_write_concern() {
    let write_concern = WriteConcern::default();
    assert!(!write_concern.inner().is_null());
}
