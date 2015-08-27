use mongo_driver::FlagsValue;

#[test]
pub fn test_insert_flags() {
    let mut flags = super::Flags::new();
    assert_eq!(0, flags.flags());

    flags.add(super::InsertFlag::ContinueOnError);
    assert_eq!(1, flags.flags());

    flags.add(super::InsertFlag::NoValidate);
    flags.add(super::InsertFlag::NoValidate);
    assert_eq!(31, flags.flags());
}

#[test]
pub fn test_query_flags() {
    let mut flags = super::Flags::new();
    assert_eq!(0, flags.flags());

    flags.add(super::QueryFlag::TailableCursor);
    assert_eq!(2, flags.flags());

    flags.add(super::QueryFlag::Partial);
    flags.add(super::QueryFlag::Partial);
    assert_eq!(130, flags.flags());
}

#[test]
pub fn test_remove_flags() {
    let mut flags = super::Flags::new();
    assert_eq!(0, flags.flags());

    flags.add(super::RemoveFlag::SingleRemove);
    flags.add(super::RemoveFlag::SingleRemove);
    assert_eq!(1, flags.flags());
}
