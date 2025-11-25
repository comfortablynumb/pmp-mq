use crate::{Event, MqError, Result};
use jsonpath_lib::select;
use regex::Regex;
use serde_json::Value;

/// Filter expression for subscription
#[derive(Debug, Clone)]
pub enum FilterExpression {
    /// JSONPath filter: $.payload.action == 'signup'
    JsonPath {
        path: String,
        operator: FilterOperator,
        value: Value,
    },

    /// Event type filter: event_type matches regex
    EventType(Regex),

    /// Topic filter: topic matches pattern
    Topic(Regex),

    /// Logical AND of filters
    And(Vec<FilterExpression>),

    /// Logical OR of filters
    Or(Vec<FilterExpression>),

    /// Logical NOT
    Not(Box<FilterExpression>),

    /// Always pass
    Always,
}

#[derive(Debug, Clone)]
pub enum FilterOperator {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    Contains,
    Matches, // Regex match
}

impl FilterExpression {
    /// Parse a filter from string format
    /// Supports:
    /// - "$.payload.action == 'signup'" (JSONPath)
    /// - "event_type == 'user.created'" (simple equality)
    /// - "topic matches 'user\..*'" (regex)
    pub fn parse(filter_str: &str) -> Result<Self> {
        let filter_str = filter_str.trim();

        if filter_str.is_empty() || filter_str == "*" {
            return Ok(FilterExpression::Always);
        }

        // Check for event_type filter
        if filter_str.starts_with("event_type") {
            if let Some(value) = Self::extract_simple_value(filter_str, "event_type") {
                let regex = Regex::new(&format!("^{}$", regex::escape(&value)))
                    .map_err(|e| MqError::ConfigError(format!("Invalid regex: {}", e)))?;
                return Ok(FilterExpression::EventType(regex));
            }
        }

        // Check for topic filter
        if filter_str.starts_with("topic") {
            if let Some(value) = Self::extract_simple_value(filter_str, "topic") {
                let regex = Regex::new(&value)
                    .map_err(|e| MqError::ConfigError(format!("Invalid regex: {}", e)))?;
                return Ok(FilterExpression::Topic(regex));
            }
        }

        // Check for JSONPath filter
        if filter_str.starts_with("$.") || filter_str.starts_with("$[") {
            return Self::parse_jsonpath(filter_str);
        }

        // Check for logical operators
        if filter_str.contains(" AND ") {
            let parts: Vec<&str> = filter_str.split(" AND ").collect();
            let filters: Result<Vec<_>> = parts.iter().map(|p| Self::parse(p)).collect();
            return Ok(FilterExpression::And(filters?));
        }

        if filter_str.contains(" OR ") {
            let parts: Vec<&str> = filter_str.split(" OR ").collect();
            let filters: Result<Vec<_>> = parts.iter().map(|p| Self::parse(p)).collect();
            return Ok(FilterExpression::Or(filters?));
        }

        Err(MqError::ConfigError(format!(
            "Invalid filter expression: {}",
            filter_str
        )))
    }

    fn extract_simple_value(filter_str: &str, prefix: &str) -> Option<String> {
        // Supports: "event_type == 'value'" or "event_type matches 'pattern'"
        if let Some(rest) = filter_str.strip_prefix(prefix) {
            let rest = rest.trim();
            if let Some(rest) = rest
                .strip_prefix("==")
                .or_else(|| rest.strip_prefix("matches"))
            {
                let value = rest.trim().trim_matches(|c| c == '\'' || c == '"');
                return Some(value.to_string());
            }
        }
        None
    }

    fn parse_jsonpath(filter_str: &str) -> Result<Self> {
        // Parse: $.payload.action == 'signup'
        let parts: Vec<&str> = filter_str.split_whitespace().collect();
        if parts.len() < 3 {
            return Err(MqError::ConfigError(format!(
                "Invalid JSONPath filter: {}",
                filter_str
            )));
        }

        let path = parts[0].to_string();
        let operator = match parts[1] {
            "==" => FilterOperator::Equals,
            "!=" => FilterOperator::NotEquals,
            ">" => FilterOperator::GreaterThan,
            "<" => FilterOperator::LessThan,
            ">=" => FilterOperator::GreaterThanOrEqual,
            "<=" => FilterOperator::LessThanOrEqual,
            "contains" => FilterOperator::Contains,
            "matches" => FilterOperator::Matches,
            _ => {
                return Err(MqError::ConfigError(format!(
                    "Unknown operator: {}",
                    parts[1]
                )))
            }
        };

        let value_str = parts[2..]
            .join(" ")
            .trim_matches(|c| c == '\'' || c == '"')
            .to_string();
        let value = serde_json::from_str(&value_str).unwrap_or(Value::String(value_str));

        Ok(FilterExpression::JsonPath {
            path,
            operator,
            value,
        })
    }

    /// Evaluate filter against an event
    pub fn matches(&self, event: &Event) -> bool {
        match self {
            FilterExpression::Always => true,

            FilterExpression::EventType(regex) => event
                .event_type
                .as_ref()
                .map_or(false, |et| regex.is_match(et)),

            FilterExpression::Topic(regex) => regex.is_match(&event.topic),

            FilterExpression::JsonPath {
                path,
                operator,
                value,
            } => Self::evaluate_jsonpath(event, path, operator, value),

            FilterExpression::And(filters) => filters.iter().all(|f| f.matches(event)),

            FilterExpression::Or(filters) => filters.iter().any(|f| f.matches(event)),

            FilterExpression::Not(filter) => !filter.matches(event),
        }
    }

    fn evaluate_jsonpath(
        event: &Event,
        path: &str,
        operator: &FilterOperator,
        expected: &Value,
    ) -> bool {
        // Combine payload and metadata for JSONPath evaluation
        let mut context = event.payload.clone();
        if let Value::Object(ref mut map) = context {
            map.insert("metadata".to_string(), event.metadata.clone());
            map.insert("topic".to_string(), Value::String(event.topic.clone()));
            if let Some(ref et) = event.event_type {
                map.insert("event_type".to_string(), Value::String(et.clone()));
            }
        }

        // Execute JSONPath
        let results = select(&context, path).ok();
        if results.is_none() {
            return false;
        }

        let results = results.unwrap();
        if results.is_empty() {
            return false;
        }

        let actual = &results[0];

        match operator {
            FilterOperator::Equals => *actual == expected,
            FilterOperator::NotEquals => *actual != expected,
            FilterOperator::GreaterThan => Self::compare_values(actual, expected, |a, b| a > b),
            FilterOperator::LessThan => Self::compare_values(actual, expected, |a, b| a < b),
            FilterOperator::GreaterThanOrEqual => {
                Self::compare_values(actual, expected, |a, b| a >= b)
            }
            FilterOperator::LessThanOrEqual => {
                Self::compare_values(actual, expected, |a, b| a <= b)
            }
            FilterOperator::Contains => {
                if let (Value::String(s), Value::String(pattern)) = (actual, expected) {
                    s.contains(pattern)
                } else if let (Value::Array(arr), _) = (actual, expected) {
                    arr.contains(expected)
                } else {
                    false
                }
            }
            FilterOperator::Matches => {
                if let (Value::String(s), Value::String(pattern)) = (actual, expected) {
                    Regex::new(pattern).map_or(false, |re| re.is_match(s))
                } else {
                    false
                }
            }
        }
    }

    fn compare_values<F>(a: &Value, b: &Value, cmp: F) -> bool
    where
        F: Fn(f64, f64) -> bool,
    {
        match (a, b) {
            (Value::Number(n1), Value::Number(n2)) => {
                let f1 = n1.as_f64().unwrap_or(0.0);
                let f2 = n2.as_f64().unwrap_or(0.0);
                cmp(f1, f2)
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn create_test_event() -> Event {
        Event {
            id: Uuid::new_v4(),
            topic: "user.events".to_string(),
            payload: serde_json::json!({
                "action": "signup",
                "user_id": "123",
                "email": "test@example.com",
                "age": 25
            }),
            metadata: serde_json::json!({"source": "api"}),
            created_at: Utc::now(),
            event_type: Some("user.created".to_string()),
        }
    }

    #[test]
    fn test_always_filter() {
        let filter = FilterExpression::Always;
        let event = create_test_event();
        assert!(filter.matches(&event));
    }

    #[test]
    fn test_event_type_filter() {
        let filter = FilterExpression::parse("event_type == 'user.created'").unwrap();
        let event = create_test_event();
        assert!(filter.matches(&event));
    }

    #[test]
    fn test_jsonpath_equals() {
        let filter = FilterExpression::parse("$.action == 'signup'").unwrap();
        let event = create_test_event();
        assert!(filter.matches(&event));
    }

    #[test]
    fn test_jsonpath_greater_than() {
        let filter = FilterExpression::parse("$.age > 18").unwrap();
        let event = create_test_event();
        assert!(filter.matches(&event));
    }

    #[test]
    fn test_jsonpath_contains() {
        let filter = FilterExpression::parse("$.email contains '@example.com'").unwrap();
        let event = create_test_event();
        assert!(filter.matches(&event));
    }
}
