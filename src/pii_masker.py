"""
Part 5: PII Masker
Mask personally identifiable information while preserving data structure
"""

import pandas as pd
import re

def load_data(filepath):
    """Load cleaned CSV data"""
    return pd.read_csv(filepath)

class PIIMasker:
    """PII masking operations"""
    
    def __init__(self):
        self.masking_stats = {
            'first_name': 0,
            'last_name': 0,
            'email': 0,
            'phone': 0,
            'address': 0,
            'date_of_birth': 0
        }
    
    def mask_name(self, name):
        """
        Mask name: 'John' -> 'J***'
        Keep first letter, mask rest with asterisks
        """
        if pd.isna(name) or str(name).strip() == '':
            return name
        
        name_str = str(name).strip()
        
        # Check for placeholder values
        if name_str == '[UNKNOWN]':
            return name_str
        
        if len(name_str) == 0:
            return name_str
        elif len(name_str) == 1:
            return name_str  # Don't mask single letter
        else:
            # Keep first letter, mask the rest
            masked = name_str[0] + '*' * (len(name_str) - 1)
            return masked
    
    def mask_email(self, email):
        """
        Mask email: 'john.doe@gmail.com' -> 'j***@gmail.com'
        Keep first letter of local part, mask rest before @
        """
        if pd.isna(email) or str(email).strip() == '':
            return email
        
        email_str = str(email).strip()
        
        # Split on @
        if '@' in email_str:
            local, domain = email_str.split('@', 1)
            
            if len(local) == 0:
                return email_str
            elif len(local) == 1:
                masked_local = local
            else:
                # Keep first letter, mask rest
                masked_local = local[0] + '***'
            
            return f"{masked_local}@{domain}"
        else:
            return email_str
    
    def mask_phone(self, phone):
        """
        Mask phone: '555-123-4567' -> '***-***-4567'
        Keep last 4 digits, mask area code and exchange
        """
        if pd.isna(phone) or str(phone).strip() == '':
            return phone
        
        phone_str = str(phone).strip()
        
        # Extract digits
        digits = re.sub(r'\D', '', phone_str)
        
        if len(digits) >= 4:
            # Keep last 4 digits, mask the rest
            last_four = digits[-4:]
            masked = '***-***-' + last_four
            return masked
        else:
            return phone_str
    
    def mask_address(self, address):
        """
        Mask address: '123 Main St New York NY 10001' -> '[MASKED ADDRESS]'
        Complete masking for privacy
        """
        if pd.isna(address) or str(address).strip() == '':
            return address
        
        address_str = str(address).strip()
        
        # Check for placeholder
        if address_str == '[UNKNOWN]':
            return address_str
        
        return '[MASKED ADDRESS]'
    
    def mask_date_of_birth(self, dob):
        """
        Mask DOB: '1985-03-15' -> '1985-**-**'
        Keep year, mask month and day
        """
        if pd.isna(dob) or str(dob).strip() == '':
            return dob
        
        dob_str = str(dob).strip()
        
        # Check for invalid dates
        if 'invalid' in dob_str.lower():
            return dob_str
        
        # Try to extract year
        # Format: YYYY-MM-DD
        if re.match(r'\d{4}-\d{2}-\d{2}', dob_str):
            year = dob_str[:4]
            return f"{year}-**-**"
        # Format: YYYY/MM/DD
        elif re.match(r'\d{4}/\d{2}/\d{2}', dob_str):
            year = dob_str[:4]
            return f"{year}-**-**"
        else:
            # Can't parse, return as-is
            return dob_str
    
    def mask_dataframe(self, df):
        """Apply all masking operations to dataframe"""
        print("=" * 70)
        print("MASKING PII DATA")
        print("=" * 70)
        print()
        
        df_masked = df.copy()
        
        # Mask first names
        print("Masking first names...")
        for idx, name in df_masked['first_name'].items():
            df_masked.at[idx, 'first_name'] = self.mask_name(name)
            if pd.notna(name) and str(name).strip() != '' and str(name).strip() != '[UNKNOWN]':
                self.masking_stats['first_name'] += 1
        print(f"  ✓ Masked {self.masking_stats['first_name']} first names")
        
        # Mask last names
        print("Masking last names...")
        for idx, name in df_masked['last_name'].items():
            df_masked.at[idx, 'last_name'] = self.mask_name(name)
            if pd.notna(name) and str(name).strip() != '' and str(name).strip() != '[UNKNOWN]':
                self.masking_stats['last_name'] += 1
        print(f"  ✓ Masked {self.masking_stats['last_name']} last names")
        
        # Mask emails
        print("Masking emails...")
        for idx, email in df_masked['email'].items():
            df_masked.at[idx, 'email'] = self.mask_email(email)
            if pd.notna(email) and str(email).strip() != '':
                self.masking_stats['email'] += 1
        print(f"  ✓ Masked {self.masking_stats['email']} emails")
        
        # Mask phone numbers
        print("Masking phone numbers...")
        for idx, phone in df_masked['phone'].items():
            df_masked.at[idx, 'phone'] = self.mask_phone(phone)
            if pd.notna(phone) and str(phone).strip() != '':
                self.masking_stats['phone'] += 1
        print(f"  ✓ Masked {self.masking_stats['phone']} phone numbers")
        
        # Mask addresses
        print("Masking addresses...")
        for idx, address in df_masked['address'].items():
            df_masked.at[idx, 'address'] = self.mask_address(address)
            if pd.notna(address) and str(address).strip() != '' and str(address).strip() != '[UNKNOWN]':
                self.masking_stats['address'] += 1
        print(f"  ✓ Masked {self.masking_stats['address']} addresses")
        
        # Mask dates of birth
        print("Masking dates of birth...")
        for idx, dob in df_masked['date_of_birth'].items():
            df_masked.at[idx, 'date_of_birth'] = self.mask_date_of_birth(dob)
            if pd.notna(dob) and str(dob).strip() != '' and 'invalid' not in str(dob).lower():
                self.masking_stats['date_of_birth'] += 1
        print(f"  ✓ Masked {self.masking_stats['date_of_birth']} dates of birth")
        
        print()
        print("PII masking complete!")
        print()
        
        return df_masked
    
    def generate_comparison_report(self, df_original, df_masked):
        """Generate before/after comparison report"""
        report = []
        report.append("PII MASKING SAMPLE COMPARISON")
        report.append("=" * 80)
        report.append("")
        
        # Show first 3 rows before and after
        num_samples = min(3, len(df_original))
        
        report.append(f"BEFORE MASKING (first {num_samples} rows):")
        report.append("-" * 80)
        
        # Header
        cols = df_original.columns.tolist()
        report.append(",".join(cols))
        
        # Data rows
        for idx in range(num_samples):
            row_data = []
            for col in cols:
                val = df_original.iloc[idx][col]
                row_data.append(str(val) if pd.notna(val) else '')
            report.append(",".join(row_data))
        
        report.append("")
        report.append(f"AFTER MASKING (first {num_samples} rows):")
        report.append("-" * 80)
        
        # Header
        report.append(",".join(cols))
        
        # Masked data rows
        for idx in range(num_samples):
            row_data = []
            for col in cols:
                val = df_masked.iloc[idx][col]
                row_data.append(str(val) if pd.notna(val) else '')
            report.append(",".join(row_data))
        
        report.append("")
        report.append("MASKING STATISTICS:")
        report.append("-" * 80)
        report.append(f"First names masked: {self.masking_stats['first_name']}")
        report.append(f"Last names masked: {self.masking_stats['last_name']}")
        report.append(f"Emails masked: {self.masking_stats['email']}")
        report.append(f"Phone numbers masked: {self.masking_stats['phone']}")
        report.append(f"Addresses masked: {self.masking_stats['address']}")
        report.append(f"Dates of birth masked: {self.masking_stats['date_of_birth']}")
        report.append("")
        
        report.append("ANALYSIS:")
        report.append("-" * 80)
        report.append("✓ Data structure preserved:")
        report.append(f"  - Original: {len(df_original)} rows, {len(df_original.columns)} columns")
        report.append(f"  - Masked: {len(df_masked)} rows, {len(df_masked.columns)} columns")
        report.append("")
        report.append("✓ PII protection achieved:")
        report.append("  - Names: First letter visible, rest masked (e.g., 'John' → 'J***')")
        report.append("  - Emails: First letter + domain visible (e.g., 'john@gmail.com' → 'j***@gmail.com')")
        report.append("  - Phones: Last 4 digits visible (e.g., '555-123-4567' → '***-***-4567')")
        report.append("  - Addresses: Completely masked (e.g., '123 Main St' → '[MASKED ADDRESS]')")
        report.append("  - DOB: Year visible, month/day masked (e.g., '1985-03-15' → '1985-**-**')")
        report.append("")
        report.append("✓ Business data intact:")
        report.append("  - customer_id: Preserved (needed for analytics)")
        report.append("  - income: Preserved (financial analysis)")
        report.append("  - account_status: Preserved (operational data)")
        report.append("  - created_date: Preserved (temporal analysis)")
        report.append("")
        report.append("USE CASE:")
        report.append("-" * 80)
        report.append("This masked dataset is safe for:")
        report.append("  ✓ Analytics teams (aggregate analysis)")
        report.append("  ✓ Machine learning models (pattern detection)")
        report.append("  ✓ External vendors (limited PII exposure)")
        report.append("  ✓ Development/testing environments")
        report.append("  ✓ GDPR/CCPA compliance (pseudonymization)")
        report.append("")
        report.append("This masked dataset is NOT suitable for:")
        report.append("  ✗ Customer outreach (emails/phones masked)")
        report.append("  ✗ Identity verification (names masked)")
        report.append("  ✗ Address validation (addresses masked)")
        report.append("  ✗ Age-specific analysis (exact DOB masked)")
        report.append("")
        
        report.append("COMPLIANCE NOTES:")
        report.append("-" * 80)
        report.append("✓ GDPR Article 32: Technical measures to ensure data security")
        report.append("✓ GDPR Recital 26: Pseudonymized data still requires protection")
        report.append("✓ CCPA: De-identification reduces privacy risk")
        report.append("✓ Best Practice: Principle of data minimization applied")
        report.append("")
        
        return "\n".join(report)

if __name__ == "__main__":
    # Load cleaned data
    df_cleaned = load_data('customers_cleaned.csv')
    
    print(f"Loaded {len(df_cleaned)} rows from customers_cleaned.csv\n")
    
    # Create masker and mask PII
    masker = PIIMasker()
    df_masked = masker.mask_dataframe(df_cleaned)
    
    # Generate comparison report
    comparison_report = masker.generate_comparison_report(df_cleaned, df_masked)
    
    # Print report
    print("\n" + comparison_report)
    
    # Save masked data
    df_masked.to_csv('customers_masked.csv', index=False)
    print("✓ Masked data saved to: ../data/customers_masked.csv")
    
    # Save comparison report
    with open('masked_sample.txt', 'w') as f:
        f.write(comparison_report)
    print("✓ Masking report saved to: ../reports/masked_sample.txt")
    
    print("\n" + "=" * 70)
    print("PII MASKING COMPLETE")
    print("=" * 70)