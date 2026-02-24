"""
Part 1: Exploratory Data Quality Analysis
Profile the raw data to understand quality issues
"""
 
import pandas as pd
import numpy as np
from datetime import datetime
import re

def load_data(filepath):
    """Load raw CSV data"""
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.strip()  # Remove leading/trailing spaces from column names
    return df

def analyze_completeness(df):
    """Calculate completeness percentage for each column"""
    completeness = {}
    total_rows = len(df)
    
    for col in df.columns:
        # Count non-null values (accounting for empty strings)
        non_null = df[col].notna().sum()
        non_empty = (df[col].astype(str).str.strip() != '').sum()
        actual_complete = min(non_null, non_empty)
        
        completeness[col] = {
            'percentage': round((actual_complete / total_rows) * 100, 1),
            'missing_count': total_rows - actual_complete
        }
    
    return completeness

def analyze_data_types(df):
    """Detect actual vs expected data types"""
    type_analysis = {}
    
    for col in df.columns:
        detected_type = str(df[col].dtype)
        
        # Determine if it's the correct type
        expected_types = {
            'customer_id': 'int',
            'first_name': 'object',
            'last_name': 'object',
            'email': 'object',
            'phone': 'object',
            'date_of_birth': 'datetime',
            'address': 'object',
            'income': 'float',
            'account_status': 'object',
            'created_date': 'datetime'
        }
        
        expected = expected_types.get(col, 'object')
        is_correct = 'int' in detected_type if expected == 'int' else \
                     'float' in detected_type if expected == 'float' else \
                     'datetime' in detected_type if expected == 'datetime' else \
                     'object' in detected_type
        
        type_analysis[col] = {
            'detected': detected_type,
            'expected': expected,
            'correct': is_correct
        }
    
    return type_analysis

def find_format_issues(df):
    """Identify format inconsistencies"""
    issues = []
    
    # Check phone formats
    phone_formats = df['phone'].dropna().astype(str).apply(lambda x: x.strip())
    unique_formats = {}
    for phone in phone_formats:
        pattern = re.sub(r'\d', 'X', phone)
        unique_formats[pattern] = unique_formats.get(pattern, 0) + 1
    
    if len(unique_formats) > 1:
        issues.append({
            'column': 'phone',
            'issue': 'Multiple formats detected',
            'formats': unique_formats,
            'examples': phone_formats.head(5).tolist()
        })
    
    # Check date formats
    dob_formats = df['date_of_birth'].dropna().astype(str).apply(lambda x: x.strip())
    date_patterns = {}
    for date in dob_formats:
        if 'invalid' in date.lower():
            pattern = 'INVALID'
        elif '/' in date:
            pattern = 'M/D/Y or Y/M/D'
        elif '-' in date:
            pattern = 'Y-M-D'
        else:
            pattern = 'UNKNOWN'
        date_patterns[pattern] = date_patterns.get(pattern, 0) + 1
    
    if len(date_patterns) > 1 or 'INVALID' in date_patterns:
        issues.append({
            'column': 'date_of_birth',
            'issue': 'Multiple/invalid date formats',
            'formats': date_patterns,
            'examples': dob_formats.head(6).tolist()
        })
    
    # Check created_date formats
    created_formats = df['created_date'].dropna().astype(str).apply(lambda x: x.strip())
    created_patterns = {}
    for date in created_formats:
        if 'invalid' in date.lower():
            pattern = 'INVALID'
        elif '/' in date:
            pattern = 'M/D/Y'
        elif '-' in date:
            pattern = 'Y-M-D'
        else:
            pattern = 'UNKNOWN'
        created_patterns[pattern] = created_patterns.get(pattern, 0) + 1
    
    if len(created_patterns) > 1 or 'INVALID' in created_patterns:
        issues.append({
            'column': 'created_date',
            'issue': 'Multiple/invalid date formats',
            'formats': created_patterns,
            'examples': created_formats.tail(2).tolist()
        })
    
    return issues

def check_uniqueness(df):
    """Check if customer_id is unique"""
    total = len(df)
    unique = df['customer_id'].nunique()
    return {
        'is_unique': total == unique,
        'total': total,
        'unique': unique,
        'duplicates': total - unique
    }

def find_invalid_values(df):
    """Find invalid values in the dataset"""
    invalid = []
    
    # Check for invalid dates
    invalid_dates_dob = df[df['date_of_birth'].astype(str).str.contains('invalid', case=False, na=False)]
    if not invalid_dates_dob.empty:
        invalid.append({
            'type': 'Invalid date in date_of_birth',
            'count': len(invalid_dates_dob),
            'rows': invalid_dates_dob.index.tolist(),
            'examples': invalid_dates_dob['date_of_birth'].tolist()
        })
    
    invalid_dates_created = df[df['created_date'].astype(str).str.contains('invalid', case=False, na=False)]
    if not invalid_dates_created.empty:
        invalid.append({
            'type': 'Invalid date in created_date',
            'count': len(invalid_dates_created),
            'rows': invalid_dates_created.index.tolist(),
            'examples': invalid_dates_created['created_date'].tolist()
        })
    
    # Check for negative income
    df['income_numeric'] = pd.to_numeric(df['income'], errors='coerce')
    negative_income = df[df['income_numeric'] < 0]
    if not negative_income.empty:
        invalid.append({
            'type': 'Negative income',
            'count': len(negative_income),
            'rows': negative_income.index.tolist()
        })
    
    # Check for ages > 150 or future DOB
    current_year = 2024
    for idx, dob in df['date_of_birth'].items():
        if pd.notna(dob) and 'invalid' not in str(dob).lower():
            try:
                # Try different date formats
                for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y']:
                    try:
                        date_obj = datetime.strptime(str(dob), fmt)
                        age = current_year - date_obj.year
                        if age > 150:
                            invalid.append({
                                'type': 'Age > 150 years',
                                'count': 1,
                                'rows': [idx],
                                'examples': [f"{dob} (age {age})"]
                            })
                        elif date_obj.year > current_year:
                            invalid.append({
                                'type': 'Future date of birth',
                                'count': 1,
                                'rows': [idx],
                                'examples': [dob]
                            })
                        break
                    except:
                        continue
            except:
                pass
    
    return invalid

def check_categorical_validity(df):
    """Check if categorical values are valid"""
    issues = []
    
    # Valid account statuses
    valid_statuses = ['active', 'inactive', 'suspended']
    
    # Find invalid or missing statuses
    df['account_status_clean'] = df['account_status'].astype(str).str.strip().str.lower()
    invalid_statuses = df[~df['account_status_clean'].isin(valid_statuses + ['nan', ''])]
    
    if not invalid_statuses.empty:
        issues.append({
            'column': 'account_status',
            'issue': 'Invalid values',
            'valid_values': valid_statuses,
            'invalid_count': len(invalid_statuses),
            'invalid_examples': invalid_statuses['account_status'].tolist()
        })
    
    # Check for empty account_status
    empty_status = df[df['account_status'].isna() | (df['account_status'].astype(str).str.strip() == '')]
    if not empty_status.empty:
        issues.append({
            'column': 'account_status',
            'issue': 'Missing/empty values',
            'count': len(empty_status),
            'rows': empty_status.index.tolist()
        })
    
    return issues

def generate_report(df):
    """Generate comprehensive data quality report"""
    
    print("Analyzing data quality...")
    
    # Run all analyses
    completeness = analyze_completeness(df)
    data_types = analyze_data_types(df)
    format_issues = find_format_issues(df)
    uniqueness = check_uniqueness(df)
    invalid_values = find_invalid_values(df)
    categorical_issues = check_categorical_validity(df)
    
    # Build report
    report = []
    report.append("DATA QUALITY PROFILE REPORT")
    report.append("=" * 50)
    report.append("")
    
    # Completeness section
    report.append("COMPLETENESS:")
    report.append("-" * 50)
    for col, stats in completeness.items():
        status = "✓" if stats['percentage'] == 100.0 else "X"
        missing_info = f"({stats['missing_count']} missing)" if stats['missing_count'] > 0 else ""
        report.append(f"- {col}: {stats['percentage']}% {status} {missing_info}")
    report.append("")
    
    # Data types section
    report.append("DATA TYPES:")
    report.append("-" * 50)
    for col, info in data_types.items():
        status = "✓" if info['correct'] else "X"
        report.append(f"- {col}: {info['detected'].upper()} {status} (expected: {info['expected'].upper()})")
    report.append("")
    
    # Uniqueness section
    report.append("UNIQUENESS:")
    report.append("-" * 50)
    status = "✓" if uniqueness['is_unique'] else "X"
    report.append(f"- customer_id: {uniqueness['unique']}/{uniqueness['total']} unique {status}")
    if uniqueness['duplicates'] > 0:
        report.append(f"  WARNING: {uniqueness['duplicates']} duplicate(s) found!")
    report.append("")
    
    # Quality issues section
    report.append("QUALITY ISSUES:")
    report.append("-" * 50)
    
    issue_num = 1
    
    # Format issues
    for issue in format_issues:
        report.append(f"{issue_num}. {issue['column'].upper()}: {issue['issue']}")
        report.append(f"   Formats found: {issue['formats']}")
        report.append(f"   Examples: {issue['examples'][:3]}")
        report.append("")
        issue_num += 1
    
    # Invalid values
    for issue in invalid_values:
        report.append(f"{issue_num}. {issue['type']}")
        report.append(f"   Count: {issue['count']}")
        report.append(f"   Rows: {issue['rows']}")
        if 'examples' in issue:
            report.append(f"   Examples: {issue['examples']}")
        report.append("")
        issue_num += 1
    
    # Categorical issues
    for issue in categorical_issues:
        report.append(f"{issue_num}. {issue['column'].upper()}: {issue['issue']}")
        if 'valid_values' in issue:
            report.append(f"   Valid values: {issue['valid_values']}")
        if 'invalid_examples' in issue:
            report.append(f"   Invalid examples: {issue['invalid_examples']}")
        if 'count' in issue:
            report.append(f"   Affected rows: {issue['rows']}")
        report.append("")
        issue_num += 1
    
    # Severity assessment
    report.append("SEVERITY ASSESSMENT:")
    report.append("-" * 50)
    
    critical = 0
    high = 0
    medium = 0
    
    # Count severity
    if not uniqueness['is_unique']:
        critical += 1
    
    for issue in invalid_values:
        if 'Invalid date' in issue['type']:
            high += issue['count']
        elif 'Negative income' in issue['type']:
            high += issue['count']
    
    for issue in categorical_issues:
        if 'Missing' in issue['issue']:
            medium += issue.get('count', 0)
    
    for issue in format_issues:
        medium += 1
    
    report.append(f"- Critical (blocks processing): {critical}")
    report.append(f"- High (data incorrect): {high}")
    report.append(f"- Medium (needs cleaning): {medium}")
    report.append("")
    
    # Overall impact
    total_rows = len(df)
    report.append("ESTIMATED IMPACT:")
    report.append("-" * 50)
    
    missing_first_name = completeness['first_name']['missing_count']
    missing_last_name = completeness['last_name']['missing_count']
    missing_address = completeness['address']['missing_count']
    missing_income = completeness['income']['missing_count']
    missing_account_status = completeness['account_status']['missing_count']
    
    if missing_first_name > 0:
        pct = round((missing_first_name / total_rows) * 100, 1)
        report.append(f"- {missing_first_name} rows have missing first_name = {pct}% incomplete")
    
    if missing_last_name > 0:
        pct = round((missing_last_name / total_rows) * 100, 1)
        report.append(f"- {missing_last_name} rows have missing last_name = {pct}% incomplete")
    
    if missing_address > 0:
        pct = round((missing_address / total_rows) * 100, 1)
        report.append(f"- {missing_address} rows have missing address = {pct}% incomplete")
    
    if missing_income > 0:
        pct = round((missing_income / total_rows) * 100, 1)
        report.append(f"- {missing_income} rows have missing income = {pct}% incomplete")
    
    if missing_account_status > 0:
        pct = round((missing_account_status / total_rows) * 100, 1)
        report.append(f"- {missing_account_status} rows have missing account_status = {pct}% incomplete")
    
    return "\n".join(report)

if __name__ == "__main__":
    # Load data
    df = load_data('/home/jeffmint/PII/data/customers_raw.csv')
    print(df.columns)
    
    # Generate report
    report_text = generate_report(df)
    
    # Print to console
    print(report_text)
    
    # Save to file
    with open('data_quality_report.txt', 'w') as f:
        f.write(report_text)
    
    print("\n" + "=" * 50)
    print("Report saved to: data_quality_report.txt")
    print("=" * 50)