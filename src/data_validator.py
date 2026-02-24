"""
Part 3: Data Validator
Define and apply validation rules to enforce schema constraints
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime
from collections import defaultdict

def load_data(filepath):
    """Load raw CSV data"""
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.strip()  # Remove leading/trailing spaces from column names
    return df

class DataValidator:
    """Custom data validator with schema rules"""
    
    def __init__(self):
        self.validation_results = {
            'passed': [],
            'failed': [],
            'failures_by_column': defaultdict(list),
            'total_rows': 0,
            'pass_count': 0,
            'fail_count': 0
        }
    
    def validate_customer_id(self, df):
        """Validate customer_id: unique, positive integer"""
        failures = []
        
        for idx, value in df['customer_id'].items():
            # Check if null
            if pd.isna(value):
                failures.append({
                    'row': idx,
                    'column': 'customer_id',
                    'value': value,
                    'error': 'NULL value (should be non-null)'
                })
                continue
            
            # Check if positive
            if value <= 0:
                failures.append({
                    'row': idx,
                    'column': 'customer_id',
                    'value': value,
                    'error': 'Non-positive value (should be > 0)'
                })
            
            # Check if integer
            if not isinstance(value, (int, np.integer)):
                failures.append({
                    'row': idx,
                    'column': 'customer_id',
                    'value': value,
                    'error': f'Non-integer type: {type(value).__name__}'
                })
        
        # Check uniqueness
        if df['customer_id'].duplicated().any():
            duplicates = df[df['customer_id'].duplicated(keep=False)]
            for idx in duplicates.index:
                failures.append({
                    'row': idx,
                    'column': 'customer_id',
                    'value': df.loc[idx, 'customer_id'],
                    'error': 'Duplicate customer_id (should be unique)'
                })
        
        return failures
    
    def validate_name_field(self, df, column_name):
        """Validate name fields: non-empty, 2-50 chars, alphabetic"""
        failures = []
        
        for idx, value in df[column_name].items():
            # Check if null or empty
            if pd.isna(value) or str(value).strip() == '':
                failures.append({
                    'row': idx,
                    'column': column_name,
                    'value': value,
                    'error': 'Empty/NULL (should be non-empty)'
                })
                continue
            
            name_str = str(value).strip()
            
            # Check length
            if len(name_str) < 2:
                failures.append({
                    'row': idx,
                    'column': column_name,
                    'value': value,
                    'error': f'Too short: {len(name_str)} chars (min: 2)'
                })
            elif len(name_str) > 50:
                failures.append({
                    'row': idx,
                    'column': column_name,
                    'value': value,
                    'error': f'Too long: {len(name_str)} chars (max: 50)'
                })
            
            # Check if alphabetic (allow spaces for compound names)
            if not re.match(r'^[A-Za-z\s\'-]+$', name_str):
                failures.append({
                    'row': idx,
                    'column': column_name,
                    'value': value,
                    'error': 'Non-alphabetic characters found'
                })
        
        return failures
    
    def validate_email(self, df):
        """Validate email: valid email format"""
        failures = []
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        for idx, value in df['email'].items():
            # Check if null
            if pd.isna(value) or str(value).strip() == '':
                failures.append({
                    'row': idx,
                    'column': 'email',
                    'value': value,
                    'error': 'Empty/NULL (should have valid email)'
                })
                continue
            
            email_str = str(value).strip()
            
            # Check format
            if not re.match(email_pattern, email_str):
                failures.append({
                    'row': idx,
                    'column': 'email',
                    'value': value,
                    'error': 'Invalid email format'
                })
        
        return failures
    
    def validate_phone(self, df):
        """Validate phone: reasonable format"""
        failures = []
        
        # Valid patterns
        phone_patterns = [
            r'\d{3}-\d{3}-\d{4}',           # 555-123-4567
            r'\(\d{3}\)\s*\d{3}-\d{4}',     # (555) 123-4567
            r'\d{3}\.\d{3}\.\d{4}',         # 555.123.4567
            r'\d{10}'                       # 5551234567
        ]
        
        for idx, value in df['phone'].items():
            # Check if null
            if pd.isna(value) or str(value).strip() == '':
                failures.append({
                    'row': idx,
                    'column': 'phone',
                    'value': value,
                    'error': 'Empty/NULL (should have phone number)'
                })
                continue
            
            phone_str = str(value).strip()
            
            # Check if matches any valid pattern
            valid = False
            for pattern in phone_patterns:
                if re.search(pattern, phone_str):
                    valid = True
                    break
            
            if not valid:
                failures.append({
                    'row': idx,
                    'column': 'phone',
                    'value': value,
                    'error': 'Invalid phone format (use XXX-XXX-XXXX, (XXX) XXX-XXXX, XXX.XXX.XXXX, or XXXXXXXXXX)'
                })
        
        return failures
    
    def validate_date(self, df, column_name):
        """Validate date fields: valid date in YYYY-MM-DD format"""
        failures = []
        
        for idx, value in df[column_name].items():
            # Check if null
            if pd.isna(value) or str(value).strip() == '':
                failures.append({
                    'row': idx,
                    'column': column_name,
                    'value': value,
                    'error': 'Empty/NULL (should have valid date)'
                })
                continue
            
            date_str = str(value).strip()
            
            # Check for obvious invalid strings
            if 'invalid' in date_str.lower():
                failures.append({
                    'row': idx,
                    'column': column_name,
                    'value': value,
                    'error': 'Invalid date string'
                })
                continue
            
            # Try to parse as date
            valid_format = False
            parsed_date = None
            
            # Try YYYY-MM-DD format (expected)
            try:
                parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
                valid_format = True
            except ValueError:
                pass
            
            # If not valid, check other formats
            if not valid_format:
                for fmt in ['%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        # Found valid date but wrong format
                        failures.append({
                            'row': idx,
                            'column': column_name,
                            'value': value,
                            'error': f'Wrong date format (should be YYYY-MM-DD, found {fmt})'
                        })
                        valid_format = True
                        break
                    except ValueError:
                        continue
            
            # If still not valid
            if not valid_format:
                failures.append({
                    'row': idx,
                    'column': column_name,
                    'value': value,
                    'error': 'Cannot parse as valid date'
                })
            
            # Additional check: date_of_birth should not be in future
            if column_name == 'date_of_birth' and parsed_date:
                if parsed_date > datetime.now():
                    failures.append({
                        'row': idx,
                        'column': column_name,
                        'value': value,
                        'error': 'Future date of birth (invalid)'
                    })
        
        return failures
    
    def validate_address(self, df):
        """Validate address: non-empty, 10-200 chars"""
        failures = []
        
        for idx, value in df['address'].items():
            # Check if null or empty
            if pd.isna(value) or str(value).strip() == '':
                failures.append({
                    'row': idx,
                    'column': 'address',
                    'value': value,
                    'error': 'Empty/NULL (should be non-empty)'
                })
                continue
            
            address_str = str(value).strip()
            
            # Check length
            if len(address_str) < 10:
                failures.append({
                    'row': idx,
                    'column': 'address',
                    'value': value,
                    'error': f'Too short: {len(address_str)} chars (min: 10)'
                })
            elif len(address_str) > 200:
                failures.append({
                    'row': idx,
                    'column': 'address',
                    'value': value,
                    'error': f'Too long: {len(address_str)} chars (max: 200)'
                })
        
        return failures
    
    def validate_income(self, df):
        """Validate income: non-negative, <= $10M"""
        failures = []
        
        for idx, value in df['income'].items():
            # Check if null
            if pd.isna(value) or str(value).strip() == '':
                failures.append({
                    'row': idx,
                    'column': 'income',
                    'value': value,
                    'error': 'Empty/NULL (should have income value)'
                })
                continue
            
            # Try to convert to numeric
            try:
                income_val = float(value)
            except (ValueError, TypeError):
                failures.append({
                    'row': idx,
                    'column': 'income',
                    'value': value,
                    'error': f'Non-numeric value: {value}'
                })
                continue
            
            # Check if non-negative
            if income_val < 0:
                failures.append({
                    'row': idx,
                    'column': 'income',
                    'value': value,
                    'error': f'Negative income: ${income_val:,.2f}'
                })
            
            # Check if reasonable (< $10M)
            if income_val > 10_000_000:
                failures.append({
                    'row': idx,
                    'column': 'income',
                    'value': value,
                    'error': f'Unreasonably high: ${income_val:,.2f} (max: $10M)'
                })
        
        return failures
    
    def validate_account_status(self, df):
        """Validate account_status: must be 'active', 'inactive', or 'suspended'"""
        failures = []
        valid_statuses = ['active', 'inactive', 'suspended']
        
        for idx, value in df['account_status'].items():
            # Check if null or empty
            if pd.isna(value) or str(value).strip() == '':
                failures.append({
                    'row': idx,
                    'column': 'account_status',
                    'value': value,
                    'error': f'Empty/NULL (should be one of: {", ".join(valid_statuses)})'
                })
                continue
            
            status_str = str(value).strip().lower()
            
            # Check if valid
            if status_str not in valid_statuses:
                failures.append({
                    'row': idx,
                    'column': 'account_status',
                    'value': value,
                    'error': f'Invalid status "{value}" (valid: {", ".join(valid_statuses)})'
                })
        
        return failures
    
    def validate_dataframe(self, df):
        """Run all validation rules on the dataframe"""
        print("Running validation checks...")
        
        self.validation_results['total_rows'] = len(df)
        all_failures = []
        
        # Run each validation
        print("  - Validating customer_id...")
        all_failures.extend(self.validate_customer_id(df))
        
        print("  - Validating first_name...")
        all_failures.extend(self.validate_name_field(df, 'first_name'))
        
        print("  - Validating last_name...")
        all_failures.extend(self.validate_name_field(df, 'last_name'))
        
        print("  - Validating email...")
        all_failures.extend(self.validate_email(df))
        
        print("  - Validating phone...")
        all_failures.extend(self.validate_phone(df))
        
        print("  - Validating date_of_birth...")
        all_failures.extend(self.validate_date(df, 'date_of_birth'))
        
        print("  - Validating address...")
        all_failures.extend(self.validate_address(df))
        
        print("  - Validating income...")
        all_failures.extend(self.validate_income(df))
        
        print("  - Validating account_status...")
        all_failures.extend(self.validate_account_status(df))
        
        print("  - Validating created_date...")
        all_failures.extend(self.validate_date(df, 'created_date'))
        
        # Organize results
        failed_rows = set()
        for failure in all_failures:
            row_idx = failure['row']
            failed_rows.add(row_idx)
            self.validation_results['failures_by_column'][failure['column']].append(failure)
        
        self.validation_results['failed'] = list(failed_rows)
        self.validation_results['fail_count'] = len(failed_rows)
        self.validation_results['pass_count'] = self.validation_results['total_rows'] - len(failed_rows)
        
        # Identify rows that passed all checks
        all_rows = set(range(len(df)))
        passed_rows = all_rows - failed_rows
        self.validation_results['passed'] = list(passed_rows)
        
        return self.validation_results
    
    def generate_report(self):
        """Generate validation results report"""
        report = []
        report.append("VALIDATION RESULTS")
        report.append("=" * 70)
        report.append("")
        
        # Summary
        results = self.validation_results
        pass_pct = round((results['pass_count'] / results['total_rows']) * 100, 1)
        fail_pct = round((results['fail_count'] / results['total_rows']) * 100, 1)
        
        report.append("SUMMARY:")
        report.append("-" * 70)
        report.append(f"Total rows validated: {results['total_rows']}")
        report.append(f"✓ PASS: {results['pass_count']} rows ({pass_pct}%) passed all checks")
        report.append(f"✗ FAIL: {results['fail_count']} rows ({fail_pct}%) failed validation")
        report.append("")
        
        if results['fail_count'] == 0:
            report.append("🎉 ALL VALIDATION CHECKS PASSED!")
            report.append("")
        else:
            report.append(f"⚠️  {results['fail_count']} ROWS REQUIRE ATTENTION")
            report.append("")
        
        # Failures by column
        if results['fail_count'] > 0:
            report.append("FAILURES BY COLUMN:")
            report.append("-" * 70)
            
            for column, failures in sorted(results['failures_by_column'].items()):
                report.append(f"\n{column.upper()}:")
                report.append(f"  Total failures: {len(failures)}")
                report.append("")
                
                # Group by error type
                error_groups = defaultdict(list)
                for failure in failures:
                    error_groups[failure['error']].append(failure)
                
                for error_msg, error_failures in error_groups.items():
                    report.append(f"  • {error_msg}")
                    report.append(f"    Affected rows: {[f['row'] for f in error_failures]}")
                    
                    # Show examples (max 3)
                    examples = [f"Row {f['row']}: '{f['value']}'" for f in error_failures[:3]]
                    report.append(f"    Examples: {'; '.join(examples)}")
                    report.append("")
        
        # Rows that passed
        if results['pass_count'] > 0:
            report.append("ROWS THAT PASSED ALL CHECKS:")
            report.append("-" * 70)
            report.append(f"Rows: {results['passed']}")
            report.append("")
        
        # Action items
        report.append("ACTION ITEMS:")
        report.append("-" * 70)
        if results['fail_count'] > 0:
            report.append("1. Review failed rows and determine remediation strategy")
            report.append("2. Clean data (normalize formats, handle missing values)")
            report.append("3. Re-run validation after cleaning")
            report.append("4. Document all changes in cleaning log")
        else:
            report.append("✓ No action needed - all validation checks passed!")
        report.append("")
        
        return "\n".join(report)

if __name__ == "__main__":
    import numpy as np
    
    # Load data
    df = load_data('/home/jeffmint/PII/data/customers_raw.csv')
    
    # Create validator and run checks
    validator = DataValidator()
    results = validator.validate_dataframe(df)
    
    # Generate report
    report_text = validator.generate_report()
    
    # Print to console
    print("\n" + report_text)
    
    # Save to file
    with open('validation_results.txt', 'w') as f:
        f.write(report_text)
    
    print("=" * 70)
    print("Report saved to: ../reports/validation_results.txt")
    print("=" * 70)