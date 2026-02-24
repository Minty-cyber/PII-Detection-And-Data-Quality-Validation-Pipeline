"""
Part 4: Data Cleaner
Fix data quality issues through normalization and missing value handling
"""

import pandas as pd
import re
from datetime import datetime
import numpy as np

def load_data(filepath):
    """Load raw CSV data"""
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.strip()  # Remove leading/trailing spaces from column names
    return df

class DataCleaner:
    """Data cleaning operations"""
    
    def __init__(self):
        self.cleaning_log = {
            'normalization': [],
            'missing_values': [],
            'validation_before': {},
            'validation_after': {},
            'rows_affected': set()
        }
    
    def normalize_phone_format(self, df):
        """Normalize all phone numbers to XXX-XXX-XXXX format"""
        print("Normalizing phone formats...")
        affected_count = 0
        
        for idx, phone in df['phone'].items():
            if pd.notna(phone):
                phone_str = str(phone).strip()
                original = phone_str
                
                # Extract digits only
                digits = re.sub(r'\D', '', phone_str)
                
                if len(digits) == 10:
                    # Format as XXX-XXX-XXXX
                    normalized = f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
                    
                    if normalized != original:
                        df.at[idx, 'phone'] = normalized
                        affected_count += 1
                        self.cleaning_log['rows_affected'].add(idx)
                        self.cleaning_log['normalization'].append({
                            'row': idx,
                            'column': 'phone',
                            'original': original,
                            'cleaned': normalized,
                            'action': 'Normalized to XXX-XXX-XXXX format'
                        })
        
        return df, affected_count
    
    def normalize_date_format(self, df, column_name):
        """Normalize dates to YYYY-MM-DD format"""
        print(f"Normalizing {column_name} formats...")
        affected_count = 0
        
        for idx, date_val in df[column_name].items():
            if pd.notna(date_val):
                date_str = str(date_val).strip()
                original = date_str
                
                # Skip if already 'invalid_date'
                if 'invalid' in date_str.lower():
                    continue
                
                # Try to parse and normalize
                parsed_date = None
                
                # Try different formats
                for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        break
                    except ValueError:
                        continue
                
                if parsed_date:
                    normalized = parsed_date.strftime('%Y-%m-%d')
                    
                    if normalized != original:
                        df.at[idx, column_name] = normalized
                        affected_count += 1
                        self.cleaning_log['rows_affected'].add(idx)
                        self.cleaning_log['normalization'].append({
                            'row': idx,
                            'column': column_name,
                            'original': original,
                            'cleaned': normalized,
                            'action': f'Normalized to YYYY-MM-DD format'
                        })
        
        return df, affected_count
    
    def normalize_name_case(self, df, column_name):
        """Apply title case to names"""
        print(f"Normalizing {column_name} case...")
        affected_count = 0
        
        for idx, name in df[column_name].items():
            if pd.notna(name) and str(name).strip() != '':
                name_str = str(name).strip()
                original = name_str
                
                # Apply title case
                normalized = name_str.title()
                
                if normalized != original:
                    df.at[idx, column_name] = normalized
                    affected_count += 1
                    self.cleaning_log['rows_affected'].add(idx)
                    self.cleaning_log['normalization'].append({
                        'row': idx,
                        'column': column_name,
                        'original': original,
                        'cleaned': normalized,
                        'action': 'Applied title case'
                    })
        
        return df, affected_count
    
    def handle_missing_values(self, df):
        """Handle missing values with appropriate strategies"""
        print("Handling missing values...")
        
        strategies = {
            'first_name': '[UNKNOWN]',
            'last_name': '[UNKNOWN]',
            'address': '[UNKNOWN]',
            'income': '0',
            'account_status': 'unknown'
        }
        
        for column, fill_value in strategies.items():
            # Find missing values
            missing_mask = df[column].isna() | (df[column].astype(str).str.strip() == '')
            missing_indices = df[missing_mask].index.tolist()
            
            if missing_indices:
                # Fill missing values
                df.loc[missing_mask, column] = fill_value
                
                for idx in missing_indices:
                    self.cleaning_log['rows_affected'].add(idx)
                    self.cleaning_log['missing_values'].append({
                        'row': idx,
                        'column': column,
                        'strategy': f'Filled with "{fill_value}"',
                        'reason': 'Missing value detected'
                    })
        
        return df
    
    def clean_dataframe(self, df):
        """Apply all cleaning operations"""
        print("=" * 70)
        print("STARTING DATA CLEANING")
        print("=" * 70)
        print()
        
        df_clean = df.copy()
        
        # Track counts
        phone_count = 0
        dob_count = 0
        created_count = 0
        first_name_count = 0
        last_name_count = 0
        
        # 1. Normalize phone formats
        df_clean, phone_count = self.normalize_phone_format(df_clean)
        print(f"  ✓ Normalized {phone_count} phone numbers")
        
        # 2. Normalize date formats
        df_clean, dob_count = self.normalize_date_format(df_clean, 'date_of_birth')
        print(f"  ✓ Normalized {dob_count} dates of birth")
        
        df_clean, created_count = self.normalize_date_format(df_clean, 'created_date')
        print(f"  ✓ Normalized {created_count} created dates")
        
        # 3. Normalize name case
        df_clean, first_name_count = self.normalize_name_case(df_clean, 'first_name')
        df_clean, last_name_count = self.normalize_name_case(df_clean, 'last_name')
        print(f"  ✓ Applied title case to {first_name_count + last_name_count} names")
        
        # 4. Handle missing values
        df_clean = self.handle_missing_values(df_clean)
        missing_count = len(self.cleaning_log['missing_values'])
        print(f"  ✓ Filled {missing_count} missing values")
        
        print()
        print(f"Total rows affected: {len(self.cleaning_log['rows_affected'])}")
        print()
        
        return df_clean
    
    def validate_cleaned_data(self, df):
        """Run validation on cleaned data"""
        print("Running validation on cleaned data...")
        
        # Import validator from part 3
        import sys
        sys.path.append('/home/claude/src')
        from data_validator import DataValidator
        
        validator = DataValidator()
        results = validator.validate_dataframe(df)
        
        return results
    
    def generate_cleaning_log(self, df_original, df_cleaned, validation_before, validation_after):
        """Generate comprehensive cleaning log"""
        report = []
        report.append("DATA CLEANING LOG")
        report.append("=" * 70)
        report.append("")
        
        # Summary
        report.append("SUMMARY:")
        report.append("-" * 70)
        report.append(f"Original rows: {len(df_original)}")
        report.append(f"Cleaned rows: {len(df_cleaned)}")
        report.append(f"Rows affected: {len(self.cleaning_log['rows_affected'])}")
        report.append("")
        
        # Normalization actions
        report.append("NORMALIZATION ACTIONS:")
        report.append("-" * 70)
        
        # Group by action type
        phone_norm = [x for x in self.cleaning_log['normalization'] if x['column'] == 'phone']
        dob_norm = [x for x in self.cleaning_log['normalization'] if x['column'] == 'date_of_birth']
        created_norm = [x for x in self.cleaning_log['normalization'] if x['column'] == 'created_date']
        name_norm = [x for x in self.cleaning_log['normalization'] if x['column'] in ['first_name', 'last_name']]
        
        report.append(f"1. Phone Format Normalization:")
        report.append(f"   - Converted {len(phone_norm)} phone numbers to XXX-XXX-XXXX format")
        if phone_norm:
            report.append(f"   - Affected rows: {[x['row'] for x in phone_norm]}")
            for item in phone_norm[:3]:  # Show first 3 examples
                report.append(f"     Example: Row {item['row']}: '{item['original']}' → '{item['cleaned']}'")
        report.append("")
        
        report.append(f"2. Date Format Normalization:")
        report.append(f"   - Date of Birth: {len(dob_norm)} dates converted to YYYY-MM-DD")
        if dob_norm:
            report.append(f"     Affected rows: {[x['row'] for x in dob_norm]}")
            for item in dob_norm[:2]:
                report.append(f"     Example: Row {item['row']}: '{item['original']}' → '{item['cleaned']}'")
        report.append(f"   - Created Date: {len(created_norm)} dates converted to YYYY-MM-DD")
        if created_norm:
            report.append(f"     Affected rows: {[x['row'] for x in created_norm]}")
            for item in created_norm[:2]:
                report.append(f"     Example: Row {item['row']}: '{item['original']}' → '{item['cleaned']}'")
        report.append("")
        
        report.append(f"3. Name Case Normalization:")
        report.append(f"   - Applied title case to {len(name_norm)} names")
        if name_norm:
            report.append(f"   - Affected rows: {[x['row'] for x in name_norm]}")
            for item in name_norm[:2]:
                report.append(f"     Example: Row {item['row']}: '{item['original']}' → '{item['cleaned']}'")
        report.append("")
        
        # Missing values
        report.append("MISSING VALUE HANDLING:")
        report.append("-" * 70)
        
        if self.cleaning_log['missing_values']:
            # Group by column
            by_column = {}
            for item in self.cleaning_log['missing_values']:
                col = item['column']
                if col not in by_column:
                    by_column[col] = []
                by_column[col].append(item)
            
            for column, items in sorted(by_column.items()):
                report.append(f"- {column}:")
                report.append(f"    Missing count: {len(items)}")
                report.append(f"    Strategy: {items[0]['strategy']}")
                report.append(f"    Affected rows: {[x['row'] for x in items]}")
                report.append("")
        else:
            report.append("No missing values found.")
            report.append("")
        
        # Validation comparison
        report.append("VALIDATION RESULTS:")
        report.append("-" * 70)
        
        report.append("BEFORE CLEANING:")
        report.append(f"  - Passed: {validation_before['pass_count']}/{validation_before['total_rows']} rows "
                     f"({round(validation_before['pass_count']/validation_before['total_rows']*100, 1)}%)")
        report.append(f"  - Failed: {validation_before['fail_count']}/{validation_before['total_rows']} rows "
                     f"({round(validation_before['fail_count']/validation_before['total_rows']*100, 1)}%)")
        report.append("")
        
        report.append("AFTER CLEANING:")
        report.append(f"  - Passed: {validation_after['pass_count']}/{validation_after['total_rows']} rows "
                     f"({round(validation_after['pass_count']/validation_after['total_rows']*100, 1)}%)")
        report.append(f"  - Failed: {validation_after['fail_count']}/{validation_after['total_rows']} rows "
                     f"({round(validation_after['fail_count']/validation_after['total_rows']*100, 1)}%)")
        report.append("")
        
        # Status
        if validation_after['fail_count'] == 0:
            report.append("STATUS: ✓ PASS - All validation checks passed after cleaning!")
        else:
            report.append(f"STATUS: ⚠ PARTIAL - {validation_after['fail_count']} rows still have issues")
            report.append("")
            report.append("REMAINING ISSUES:")
            if validation_after['fail_count'] > 0:
                for column, failures in validation_after['failures_by_column'].items():
                    report.append(f"  - {column}: {len(failures)} failures")
        report.append("")
        
        # Output info
        report.append("OUTPUT:")
        report.append("-" * 70)
        report.append(f"Cleaned dataset saved to: customers_cleaned.csv")
        report.append(f"Total rows: {len(df_cleaned)}")
        report.append(f"Total columns: {len(df_cleaned.columns)}")
        report.append("")
        
        return "\n".join(report)

if __name__ == "__main__":
    # Load original data
    df_original = load_data('/home/jeffmint/PII/data/customers_raw.csv')
    print(df_original.dtypes)
    
    # Validate before cleaning
    print("Validating original data...")
    import sys
    sys.path.append('/home/claude/src')
    from data_validator import DataValidator
    
    validator_before = DataValidator()
    validation_before = validator_before.validate_dataframe(df_original)
    print(f"Before cleaning: {validation_before['pass_count']}/{validation_before['total_rows']} passed\n")
    
    # Clean the data
    cleaner = DataCleaner()
    df_cleaned = cleaner.clean_dataframe(df_original)
    
    # Validate after cleaning
    validation_after = cleaner.validate_cleaned_data(df_cleaned)
    print(f"After cleaning: {validation_after['pass_count']}/{validation_after['total_rows']} passed\n")
    
    # Generate log
    log_text = cleaner.generate_cleaning_log(df_original, df_cleaned, validation_before, validation_after)
    
    # Print log
    print("\n" + log_text)
    
    # Save cleaned data
    df_cleaned.to_csv('customers_cleaned.csv', index=False)
    print("\n✓ Cleaned data saved to: ../data/customers_cleaned.csv")
    
    # Save log
    with open('cleaning_log.txt', 'w') as f:
        f.write(log_text)
    print("✓ Cleaning log saved to: ../reports/cleaning_log.txt")
    
    print("\n" + "=" * 70)
    print("DATA CLEANING COMPLETE")
    print("=" * 70)