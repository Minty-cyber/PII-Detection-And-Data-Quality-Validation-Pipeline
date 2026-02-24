"""
Part 2: PII Detection
Identify personally identifiable information in the dataset
"""

import pandas as pd
import re
from collections import defaultdict

def load_data(filepath):
    """Load raw CSV data"""
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.strip()  # Remove leading/trailing spaces from column names
    return df

def detect_email_patterns(df):
    """Detect email addresses using regex"""
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    results = {
        'column': 'email',
        'pattern': email_pattern,
        'found_count': 0,
        'total_rows': len(df),
        'examples': [],
        'coverage_pct': 0.0
    }
    
    for idx, email in df['email'].items():
        if pd.notna(email) and re.match(email_pattern, str(email).strip()):
            results['found_count'] += 1
            if len(results['examples']) < 3:
                results['examples'].append(str(email))
    
    results['coverage_pct'] = round((results['found_count'] / results['total_rows']) * 100, 1)
    
    return results

def detect_phone_patterns(df):
    """Detect phone numbers using regex"""
    # Pattern matches various formats: XXX-XXX-XXXX, (XXX) XXX-XXXX, XXX.XXX.XXXX, XXXXXXXXXX
    phone_patterns = [
        r'\d{3}-\d{3}-\d{4}',           # 555-123-4567
        r'\(\d{3}\)\s*\d{3}-\d{4}',     # (555) 123-4567
        r'\d{3}\.\d{3}\.\d{4}',         # 555.123.4567
        r'\d{10}'                       # 5551234567
    ]
    
    results = {
        'column': 'phone',
        'patterns': phone_patterns,
        'found_count': 0,
        'total_rows': len(df),
        'examples': [],
        'coverage_pct': 0.0,
        'format_breakdown': defaultdict(int)
    }
    
    for idx, phone in df['phone'].items():
        if pd.notna(phone):
            phone_str = str(phone).strip()
            matched = False
            
            for pattern in phone_patterns:
                if re.search(pattern, phone_str):
                    results['found_count'] += 1
                    matched = True
                    
                    # Track format types
                    if '-' in phone_str and '(' not in phone_str:
                        results['format_breakdown']['XXX-XXX-XXXX'] += 1
                    elif '(' in phone_str:
                        results['format_breakdown']['(XXX) XXX-XXXX'] += 1
                    elif '.' in phone_str:
                        results['format_breakdown']['XXX.XXX.XXXX'] += 1
                    else:
                        results['format_breakdown']['XXXXXXXXXX'] += 1
                    
                    if len(results['examples']) < 3:
                        results['examples'].append(phone_str)
                    break
    
    results['coverage_pct'] = round((results['found_count'] / results['total_rows']) * 100, 1)
    
    return results

def detect_names(df):
    """Detect name columns"""
    results = {
        'first_name': {
            'column': 'first_name',
            'found_count': 0,
            'total_rows': len(df),
            'examples': [],
            'coverage_pct': 0.0
        },
        'last_name': {
            'column': 'last_name',
            'found_count': 0,
            'total_rows': len(df),
            'examples': [],
            'coverage_pct': 0.0
        }
    }
    
    # Check first names
    for idx, name in df['first_name'].items():
        if pd.notna(name) and str(name).strip() != '':
            results['first_name']['found_count'] += 1
            if len(results['first_name']['examples']) < 3:
                results['first_name']['examples'].append(str(name))
    
    results['first_name']['coverage_pct'] = round(
        (results['first_name']['found_count'] / results['first_name']['total_rows']) * 100, 1
    )
    
    # Check last names
    for idx, name in df['last_name'].items():
        if pd.notna(name) and str(name).strip() != '':
            results['last_name']['found_count'] += 1
            if len(results['last_name']['examples']) < 3:
                results['last_name']['examples'].append(str(name))
    
    results['last_name']['coverage_pct'] = round(
        (results['last_name']['found_count'] / results['last_name']['total_rows']) * 100, 1
    )
    
    return results

def detect_addresses(df):
    """Detect address information"""
    results = {
        'column': 'address',
        'found_count': 0,
        'total_rows': len(df),
        'examples': [],
        'coverage_pct': 0.0
    }
    
    for idx, address in df['address'].items():
        if pd.notna(address) and str(address).strip() != '':
            results['found_count'] += 1
            if len(results['examples']) < 3:
                results['examples'].append(str(address))
    
    results['coverage_pct'] = round((results['found_count'] / results['total_rows']) * 100, 1)
    
    return results

def detect_dates_of_birth(df):
    """Detect dates of birth"""
    results = {
        'column': 'date_of_birth',
        'found_count': 0,
        'total_rows': len(df),
        'examples': [],
        'coverage_pct': 0.0,
        'valid_count': 0
    }
    
    for idx, dob in df['date_of_birth'].items():
        if pd.notna(dob):
            dob_str = str(dob).strip()
            results['found_count'] += 1
            
            # Check if it's a valid date (not "invalid_date")
            if 'invalid' not in dob_str.lower():
                results['valid_count'] += 1
            
            if len(results['examples']) < 3:
                results['examples'].append(dob_str)
    
    results['coverage_pct'] = round((results['found_count'] / results['total_rows']) * 100, 1)
    results['valid_pct'] = round((results['valid_count'] / results['total_rows']) * 100, 1)
    
    return results

def assess_pii_risk(email_results, phone_results, name_results, address_results, dob_results):
    """Assess the overall PII exposure risk"""
    
    risk_assessment = {
        'high_risk_pii': [],
        'medium_risk_pii': [],
        'exposure_scenarios': [],
        'mitigation_required': []
    }
    
    # High risk: Direct identifiers
    if email_results['found_count'] > 0:
        risk_assessment['high_risk_pii'].append(
            f"Emails: {email_results['found_count']} records ({email_results['coverage_pct']}%)"
        )
    
    if phone_results['found_count'] > 0:
        risk_assessment['high_risk_pii'].append(
            f"Phone numbers: {phone_results['found_count']} records ({phone_results['coverage_pct']}%)"
        )
    
    if name_results['first_name']['found_count'] > 0 or name_results['last_name']['found_count'] > 0:
        risk_assessment['high_risk_pii'].append(
            f"Names: {name_results['first_name']['found_count']} first names, "
            f"{name_results['last_name']['found_count']} last names"
        )
    
    if address_results['found_count'] > 0:
        risk_assessment['high_risk_pii'].append(
            f"Addresses: {address_results['found_count']} records ({address_results['coverage_pct']}%)"
        )
    
    if dob_results['valid_count'] > 0:
        risk_assessment['high_risk_pii'].append(
            f"Dates of birth: {dob_results['valid_count']} valid records ({dob_results['valid_pct']}%)"
        )
    
    # Medium risk: Financial data
    risk_assessment['medium_risk_pii'].append("Income: Financial sensitivity")
    
    # Exposure scenarios
    if email_results['found_count'] > 0:
        risk_assessment['exposure_scenarios'].append(
            "PHISHING: Attackers can send targeted phishing emails to customers"
        )
    
    if phone_results['found_count'] > 0:
        risk_assessment['exposure_scenarios'].append(
            "SOCIAL ENGINEERING: Phone numbers enable vishing (voice phishing) attacks"
        )
    
    if (name_results['first_name']['found_count'] > 0 and 
        dob_results['valid_count'] > 0 and 
        address_results['found_count'] > 0):
        risk_assessment['exposure_scenarios'].append(
            "IDENTITY THEFT: Names + DOB + Address = enough to spoof identities"
        )
    
    if email_results['found_count'] > 0 and phone_results['found_count'] > 0:
        risk_assessment['exposure_scenarios'].append(
            "ACCOUNT TAKEOVER: Email + phone = password reset vulnerability"
        )
    
    # Mitigation
    risk_assessment['mitigation_required'].append(
        "IMMEDIATE: Mask all PII before sharing with analytics teams"
    )
    risk_assessment['mitigation_required'].append(
        "REQUIRED: Implement role-based access control (RBAC)"
    )
    risk_assessment['mitigation_required'].append(
        "COMPLIANCE: Ensure GDPR/CCPA data processing agreements"
    )
    risk_assessment['mitigation_required'].append(
        "SECURITY: Encrypt data at rest and in transit"
    )
    
    return risk_assessment

def calculate_breach_impact(df, email_results, phone_results, name_results, address_results, dob_results):
    """Calculate potential impact of a data breach"""
    
    total_rows = len(df)
    
    # Calculate how many complete profiles exist
    complete_profiles = 0
    
    for idx in df.index:
        has_email = pd.notna(df.loc[idx, 'email']) and str(df.loc[idx, 'email']).strip() != ''
        has_phone = pd.notna(df.loc[idx, 'phone']) and str(df.loc[idx, 'phone']).strip() != ''
        has_first = pd.notna(df.loc[idx, 'first_name']) and str(df.loc[idx, 'first_name']).strip() != ''
        has_last = pd.notna(df.loc[idx, 'last_name']) and str(df.loc[idx, 'last_name']).strip() != ''
        has_address = pd.notna(df.loc[idx, 'address']) and str(df.loc[idx, 'address']).strip() != ''
        has_dob = pd.notna(df.loc[idx, 'date_of_birth']) and 'invalid' not in str(df.loc[idx, 'date_of_birth']).lower()
        
        # Complete profile = name + (email or phone) + (address or dob)
        if (has_first or has_last) and (has_email or has_phone) and (has_address or has_dob):
            complete_profiles += 1
    
    impact = {
        'total_records': total_rows,
        'complete_profiles': complete_profiles,
        'complete_profile_pct': round((complete_profiles / total_rows) * 100, 1),
        'estimated_damage': 'HIGH' if complete_profiles >= total_rows * 0.5 else 'MEDIUM'
    }
    
    return impact

def generate_pii_report(df):
    """Generate comprehensive PII detection report"""
    
    print("Detecting PII in dataset...")
    
    # Run detections
    email_results = detect_email_patterns(df)
    phone_results = detect_phone_patterns(df)
    name_results = detect_names(df)
    address_results = detect_addresses(df)
    dob_results = detect_dates_of_birth(df)
    
    # Assess risk
    risk_assessment = assess_pii_risk(email_results, phone_results, name_results, 
                                       address_results, dob_results)
    
    # Calculate breach impact
    breach_impact = calculate_breach_impact(df, email_results, phone_results, 
                                            name_results, address_results, dob_results)
    
    # Build report
    report = []
    report.append("PII DETECTION REPORT")
    report.append("=" * 60)
    report.append("")
    
    # Risk classification
    report.append("RISK CLASSIFICATION:")
    report.append("-" * 60)
    report.append("HIGH RISK PII (Direct Identifiers):")
    for item in risk_assessment['high_risk_pii']:
        report.append(f"  • {item}")
    report.append("")
    report.append("MEDIUM RISK PII (Sensitive Financial):")
    for item in risk_assessment['medium_risk_pii']:
        report.append(f"  • {item}")
    report.append("")
    
    # Detected PII details
    report.append("DETECTED PII DETAILS:")
    report.append("-" * 60)
    
    # Emails
    report.append(f"📧 EMAILS:")
    report.append(f"   Found: {email_results['found_count']}/{email_results['total_rows']} "
                  f"({email_results['coverage_pct']}%)")
    report.append(f"   Examples: {', '.join(email_results['examples'])}")
    report.append("")
    
    # Phone numbers
    report.append(f"📱 PHONE NUMBERS:")
    report.append(f"   Found: {phone_results['found_count']}/{phone_results['total_rows']} "
                  f"({phone_results['coverage_pct']}%)")
    report.append(f"   Format breakdown:")
    for fmt, count in phone_results['format_breakdown'].items():
        report.append(f"      - {fmt}: {count} records")
    report.append(f"   Examples: {', '.join(phone_results['examples'])}")
    report.append("")
    
    # Names
    report.append(f"👤 NAMES:")
    report.append(f"   First names: {name_results['first_name']['found_count']}/{name_results['first_name']['total_rows']} "
                  f"({name_results['first_name']['coverage_pct']}%)")
    report.append(f"   Last names: {name_results['last_name']['found_count']}/{name_results['last_name']['total_rows']} "
                  f"({name_results['last_name']['coverage_pct']}%)")
    report.append(f"   Examples: {', '.join(name_results['first_name']['examples'][:2])} / "
                  f"{', '.join(name_results['last_name']['examples'][:2])}")
    report.append("")
    
    # Addresses
    report.append(f"🏠 ADDRESSES:")
    report.append(f"   Found: {address_results['found_count']}/{address_results['total_rows']} "
                  f"({address_results['coverage_pct']}%)")
    report.append(f"   Examples: {address_results['examples'][0] if address_results['examples'] else 'N/A'}")
    report.append("")
    
    # Dates of birth
    report.append(f"🎂 DATES OF BIRTH:")
    report.append(f"   Found: {dob_results['found_count']}/{dob_results['total_rows']} "
                  f"({dob_results['coverage_pct']}%)")
    report.append(f"   Valid dates: {dob_results['valid_count']}/{dob_results['total_rows']} "
                  f"({dob_results['valid_pct']}%)")
    report.append(f"   Examples: {', '.join(dob_results['examples'][:3])}")
    report.append("")
    
    # Exposure risk
    report.append("EXPOSURE RISK ANALYSIS:")
    report.append("-" * 60)
    report.append("If this dataset were breached, attackers could:")
    for scenario in risk_assessment['exposure_scenarios']:
        report.append(f"  ⚠️  {scenario}")
    report.append("")
    
    # Breach impact
    report.append("BREACH IMPACT ASSESSMENT:")
    report.append("-" * 60)
    report.append(f"Total records exposed: {breach_impact['total_records']}")
    report.append(f"Complete profiles (name+contact+address/DOB): {breach_impact['complete_profiles']} "
                  f"({breach_impact['complete_profile_pct']}%)")
    report.append(f"Estimated damage level: {breach_impact['estimated_damage']}")
    report.append("")
    report.append("A 'complete profile' enables full identity theft, account takeover,")
    report.append("and targeted attacks. Each complete profile = high-value target.")
    report.append("")
    
    # Mitigation
    report.append("REQUIRED MITIGATION:")
    report.append("-" * 60)
    for mitigation in risk_assessment['mitigation_required']:
        report.append(f"  ✓ {mitigation}")
    report.append("")
    
    # Compliance notes
    report.append("COMPLIANCE NOTES:")
    report.append("-" * 60)
    report.append("  • GDPR: Personal data requires lawful basis for processing")
    report.append("  • GDPR: Right to erasure ('right to be forgotten') applies")
    report.append("  • CCPA: Consumers have right to know what data is collected")
    report.append("  • PCI DSS: If payment data added, additional controls required")
    report.append("  • HIPAA: If health data added, must comply with privacy rules")
    report.append("")
    
    # Recommendations
    report.append("IMMEDIATE RECOMMENDATIONS:")
    report.append("-" * 60)
    report.append("  1. MASK PII before sharing with analytics/ML teams")
    report.append("  2. Implement field-level encryption for sensitive columns")
    report.append("  3. Set up access logging (who accessed what, when)")
    report.append("  4. Create data retention policy (how long to keep PII)")
    report.append("  5. Establish incident response plan for breaches")
    report.append("  6. Conduct privacy impact assessment (PIA)")
    report.append("  7. Train employees on PII handling procedures")
    report.append("")
    
    return "\n".join(report)

if __name__ == "__main__":
    # Load data
    df = load_data('/home/jeffmint/PII/data/customers_raw.csv')
    print(df.columns)
    
    # Generate report
    report_text = generate_pii_report(df)
    
    # Print to console
    print(report_text)
    
    # Save to file
    with open('pii_detection_report.txt', 'w') as f:
        f.write(report_text)
    
    print("=" * 60)
    print("Report saved to: ../reports/pii_detection_report.txt")
    print("=" * 60)