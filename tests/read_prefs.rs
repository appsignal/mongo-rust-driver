extern crate mongo_driver;

use mongo_driver::read_prefs::ReadPrefs;

#[test]
fn test_read_prefs() {
    let read_prefs = ReadPrefs::default();
    assert!(!read_prefs.inner().is_null());
}
