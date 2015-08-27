use mongo_driver::ReadPrefs;

#[test]
fn test_read_prefs() {
    let read_prefs = ReadPrefs::default();
    assert!(!read_prefs.inner().is_null());
}
