use mongo_driver::Uri;

#[cfg(test)]
mod tests {
    #[test]
    fn test_new_uri() {
        let uri = Uri::new("mongodb://localhost:27017/");
        assert_eq!("mongodb://localhost:27017/", uri.as_str());
    }

    #[test]
    fn test_new_invalid_uri() {
        assert!(Uri::new("@:/mongo::").is_none());
    }
}
