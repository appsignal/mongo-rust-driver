use mongo_driver::flags::{Flags,FlagsValue,InsertFlag,RemoveFlag,QueryFlag};

#[test]
pub fn test_insert_flags() {
    let mut flags = Flags::new();
    assert_eq!(0, flags.flags());

    flags.add(InsertFlag::ContinueOnError);
    assert_eq!(1, flags.flags());

    flags.add(InsertFlag::NoValidate);
    flags.add(InsertFlag::NoValidate);
    assert_eq!(31, flags.flags());
}

#[test]
pub fn test_query_flags() {
    let mut flags = Flags::new();
    assert_eq!(0, flags.flags());

    flags.add(QueryFlag::TailableCursor);
    assert_eq!(2, flags.flags());

    flags.add(QueryFlag::Partial);
    flags.add(QueryFlag::Partial);
    assert_eq!(130, flags.flags());
}

#[test]
pub fn test_remove_flags() {
    let mut flags = Flags::new();
    assert_eq!(0, flags.flags());

    flags.add(RemoveFlag::SingleRemove);
    flags.add(RemoveFlag::SingleRemove);
    assert_eq!(1, flags.flags());
}
