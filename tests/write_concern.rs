use mongo_driver::WriteConcern;

#[cfg(test)]
mod tests {
    #[test]
    fn test_write_concern() {
        let write_concern = WriteConcern::new();
        assert!(!write_concern.inner().is_null());
    }
}
