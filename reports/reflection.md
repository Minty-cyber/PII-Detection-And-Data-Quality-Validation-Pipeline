# Reflection on PII Detection & Data Quality Validation Pipeline

## Biggest Data Quality Issues

### Top 5 Problems Found
1. **Missing Values**:
   - **Problem**: Several columns, such as `first_name`, `last_name`, `address`, and `income`, had missing or empty values.
   - **Fix**: Implemented a missing value handling strategy in the `DataCleaner` class. For example:
     - `first_name` and `last_name`: Filled with `[UNKNOWN]`.
     - `income`: Filled with `0`.
     - `address`: Filled with `[UNKNOWN]`.
   - **Impact**: Ensured that all rows had complete data, enabling downstream processes to run without errors. However, some data utility was lost due to the use of placeholders.

2. **Inconsistent Formats**:
   - **Problem**: Phone numbers and dates were in inconsistent formats (e.g., `555-123-4567`, `(555) 123-4567`, `5551234567` for phone numbers; `YYYY-MM-DD`, `MM/DD/YYYY`, `invalid_date` for dates).
   - **Fix**: Normalized phone numbers to the `XXX-XXX-XXXX` format and dates to the `YYYY-MM-DD` format using regular expressions and date parsing logic.
   - **Impact**: Improved data consistency, making it easier to analyze and validate the data.

3. **Invalid Values**:
   - **Problem**: Some columns contained invalid values, such as `invalid_date` in the `date_of_birth` and `created_date` columns, and negative values in the `income` column.
   - **Fix**: Replaced invalid dates with a placeholder (`invalid_date`) and flagged rows with negative income for further review.
   - **Impact**: Highlighted problematic data for further investigation and ensured invalid values did not disrupt downstream processes.

4. **Duplicate Customer IDs**:
   - **Problem**: The `customer_id` column was expected to be unique, but duplicates were detected.
   - **Fix**: Identified duplicate rows and flagged them for manual review.
   - **Impact**: Prevented potential issues with data integrity and ensured that each customer was uniquely identifiable.

5. **Invalid Categorical Values**:
   - **Problem**: The `account_status` column contained invalid or unexpected values (e.g., empty strings, values outside the expected set of `active`, `inactive`, `suspended`).
   - **Fix**: Normalized the column values to lowercase and replaced invalid or empty values with `unknown`.
   - **Impact**: Improved data quality and ensured that the column adhered to the expected schema.

---

## PII Risk Assessment

### Detected PII
- **Emails**: 100% of records contained email addresses.
- **Phone Numbers**: 100% of records contained phone numbers.
- **Names**: 90% of records contained first and last names.
- **Addresses**: 90% of records contained addresses.
- **Dates of Birth**: 90% of records contained valid dates of birth.

### Sensitivity of PII
- **Emails and Phone Numbers**: These are direct identifiers that can be used for phishing, spam, or social engineering attacks.
- **Names and Addresses**: These can be used to identify individuals and are often combined with other PII for identity theft.
- **Dates of Birth**: Sensitive information that can be used for identity verification or to infer age.

### Potential Damage if Leaked
- **Identity Theft**: Names, addresses, and dates of birth can be used to impersonate individuals.
- **Phishing and Social Engineering**: Emails and phone numbers can be exploited for scams or fraud.
- **Reputational Damage**: A data breach could harm the organization's reputation and lead to legal consequences under regulations like GDPR and CCPA.

---

## Masking Trade-offs

### Data Utility vs. Privacy
- **Reduced Utility**: Masking PII, such as emails and phone numbers, makes it impossible to contact customers directly. Masking addresses and dates of birth limits the ability to perform location-based or age-specific analytics.
- **When Masking is Worth It**:
  - When sharing data with external vendors or third-party analytics teams.
  - When complying with regulations like GDPR or CCPA.
  - When the dataset is used for machine learning or analytics where PII is not required.
- **When NOT to Mask**:
  - When the data is used for customer communication or identity verification.
  - When the dataset is used internally by teams with proper access controls.

---

## Validation Strategy

### Effectiveness of Validators
- **Strengths**:
  - Validators effectively caught issues like missing values, invalid formats, and out-of-range values.
  - Specific checks for phone number formats, email patterns, and date validity were robust.
- **Missed Issues**:
  - Some edge cases, such as names with special characters or uncommon date formats, were not caught.
  - The `income` column allowed non-numeric strings to pass as valid values.
- **Improvements**:
  - Expand regex patterns for more comprehensive validation (e.g., international phone numbers).
  - Implement stricter checks for numeric fields like `income`.
  - Add more granular error messages for easier debugging.

---

## Production Operations

### Pipeline Execution
- **Frequency**: The pipeline could run daily, hourly, or on-demand, depending on the data ingestion frequency.
- **Validation Failures**:
  - If validation fails, the pipeline should stop and notify the data engineering team.
  - Notifications can be sent via email, Slack, or a monitoring tool like PagerDuty.
- **Failure Handling**:
  - Log detailed error messages for debugging.
  - Implement retry mechanisms for transient issues (e.g., network errors).
  - For persistent issues, flag problematic data for manual review and reprocessing.

---

## Lessons Learned

### Surprises
- The extent of data quality issues in a small dataset was surprising. Even with only 10 records, there were numerous issues that required attention.
- The complexity of handling edge cases (e.g., invalid dates, mixed formats) was greater than expected.

### Challenges
- Designing robust validation rules that account for all edge cases was more difficult than anticipated.
- Balancing data utility and privacy during PII masking required careful consideration.

### Future Improvements
- Automate the pipeline to handle larger datasets and integrate with a data warehouse.
- Implement more advanced validation and cleaning techniques, such as machine learning-based anomaly detection.
- Enhance logging and monitoring to improve error handling and operational visibility.
- Explore encryption techniques for sensitive data instead of masking in certain use cases.

---

This project highlighted the importance of data quality and privacy in modern data engineering workflows. By addressing data quality issues and implementing PII protection, we can ensure compliance and build trust with stakeholders while maintaining the utility of the data for analytics and decision-making.