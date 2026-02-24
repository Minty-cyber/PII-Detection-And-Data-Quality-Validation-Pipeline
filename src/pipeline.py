"""
Part 6: End-to-End Pipeline
Orchestrate all data quality stages into a single automated workflow
"""

import pandas as pd
import sys
import os
from datetime import datetime
import traceback

# Add src directory to path for imports
sys.path.append('/home/claude/src')

# Import all previous modules
from data_quality_analysis import generate_report as generate_quality_report
from pii_detection import generate_pii_report
from data_validator import DataValidator
from data_cleaner import DataCleaner
from pii_masker import PIIMasker

class DataPipeline:
    """End-to-end data quality and PII protection pipeline"""
    
    def __init__(self, input_file, output_dir='/home/jeffmint/PII/data/', reports_dir='../reports'):
        self.input_file = input_file
        self.output_dir = output_dir
        self.reports_dir = reports_dir
        self.execution_log = []
        self.start_time = None
        self.end_time = None
        self.status = 'NOT_STARTED'
        
    def log(self, stage, status, message, details=None):
        """Log pipeline execution details"""
        log_entry = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'stage': stage,
            'status': status,
            'message': message,
            'details': details
        }
        self.execution_log.append(log_entry)
        
        # Print to console
        status_symbol = '✓' if status == 'SUCCESS' else '✗' if status == 'FAIL' else '⟳'
        print(f"{status_symbol} [{stage}] {message}")
        if details:
            print(f"   Details: {details}")
    
    def stage_1_load(self):
        """Stage 1: Load raw data"""
        try:
            self.log('STAGE 1', 'RUNNING', 'Loading raw data...')
            
            df = pd.read_csv(self.input_file)
            df.columns = df.columns.str.strip()
            rows, cols = df.shape
            
            self.log('STAGE 1', 'SUCCESS', 'Data loaded successfully',
                    f'{rows} rows, {cols} columns')
            
            return df, None
            
        except Exception as e:
            error_msg = f"Failed to load data: {str(e)}"
            self.log('STAGE 1', 'FAIL', error_msg)
            return None, error_msg
    
    def stage_2_profile(self, df):
        """Stage 2: Data quality profiling"""
        try:
            self.log('STAGE 2', 'RUNNING', 'Profiling data quality...')
            print("Columns in DataFrame:", df.columns)
            
            report = generate_quality_report(df)
            
            # Save report
            report_path = '/home/jeffmint/PII/reports/data_quality_report.txt'
            with open(report_path, 'w') as f:
                f.write(report)
            
            self.log('STAGE 2', 'SUCCESS', 'Quality profile generated',
                    f'Report saved to {report_path}')
            
            return True, None
            
        except Exception as e:
            error_msg = f"Profiling failed: {str(e)}"
            self.log('STAGE 2', 'FAIL', error_msg)
            return False, error_msg
    
    def stage_3_detect_pii(self, df):
        """Stage 3: PII detection"""
        try:
            self.log('STAGE 3', 'RUNNING', 'Detecting PII...')
            
            report = generate_pii_report(df)
            
            # Save report
            report_path = '/home/jeffmint/PII/reports/pii_detection_report.txt'
            with open(report_path, 'w') as f:
                f.write(report)
            
            self.log('STAGE 3', 'SUCCESS', 'PII detection complete',
                    f'Report saved to {report_path}')
            
            return True, None
            
        except Exception as e:
            error_msg = f"PII detection failed: {str(e)}"
            self.log('STAGE 3', 'FAIL', error_msg)
            return False, error_msg
    
    def stage_4_validate(self, df):
        """Stage 4: Data validation"""
        try:
            self.log('STAGE 4', 'RUNNING', 'Validating data against schema...')
            
            validator = DataValidator()
            results = validator.validate_dataframe(df)
            
            report = validator.generate_report()
            
            # Save report
            report_path = '/home/jeffmint/PII/reports/validation_results.txt'
            with open(report_path, 'w') as f:
                f.write(report)
            
            pass_rate = round((results['pass_count'] / results['total_rows']) * 100, 1)
            
            self.log('STAGE 4', 'SUCCESS', 'Validation complete',
                    f"{results['pass_count']}/{results['total_rows']} passed ({pass_rate}%)")
            
            return results, None
            
        except Exception as e:
            error_msg = f"Validation failed: {str(e)}"
            self.log('STAGE 4', 'FAIL', error_msg)
            return None, error_msg
    
    def stage_5_clean(self, df):
        """Stage 5: Data cleaning"""
        try:
            self.log('STAGE 5', 'RUNNING', 'Cleaning data...')
            
            cleaner = DataCleaner()
            df_cleaned = cleaner.clean_dataframe(df)
            
            # Validate cleaned data
            validation_after = cleaner.validate_cleaned_data(df_cleaned)
            
            # Generate log
            validator_before = DataValidator()
            validation_before = validator_before.validate_dataframe(df)
            
            log_report = cleaner.generate_cleaning_log(df, df_cleaned, 
                                                       validation_before, validation_after)
            
            # Save log
            log_path = os.path.join(self.reports_dir, 'cleaning_log.txt')
            with open(log_path, 'w') as f:
                f.write(log_report)
            
            # Save cleaned data
            cleaned_path = '/home/jeffmint/PII/data/customers_cleaned.csv'
            df_cleaned.to_csv(cleaned_path, index=False)
            
            affected = len(cleaner.cleaning_log['rows_affected'])
            
            self.log('STAGE 5', 'SUCCESS', 'Data cleaned',
                    f'{affected} rows affected, saved to {cleaned_path}')
            
            return df_cleaned, None
            
        except Exception as e:
            error_msg = f"Cleaning failed: {str(e)}"
            self.log('STAGE 5', 'FAIL', error_msg)
            traceback.print_exc()
            return None, error_msg
    
    def stage_6_mask_pii(self, df):
        """Stage 6: PII masking"""
        try:
            self.log('STAGE 6', 'RUNNING', 'Masking PII...')
            
            masker = PIIMasker()
            df_masked = masker.mask_dataframe(df)
            
            # Generate comparison report
            comparison = masker.generate_comparison_report(df, df_masked)
            
            # Save report
            report_path = os.path.join(self.reports_dir, 'masked_sample.txt')
            with open(report_path, 'w') as f:
                f.write(comparison)
            
            # Save masked data
            masked_path = '/home/jeffmint/PII/data/customers_masked.csv'
            df_masked.to_csv(masked_path, index=False)
            
            total_masked = sum(masker.masking_stats.values())
            
            self.log('STAGE 6', 'SUCCESS', 'PII masked',
                    f'{total_masked} fields masked, saved to {masked_path}')
            
            return df_masked, None
            
        except Exception as e:
            error_msg = f"Masking failed: {str(e)}"
            self.log('STAGE 6', 'FAIL', error_msg)
            return None, error_msg
    
    def generate_execution_report(self):
        """Generate final pipeline execution report"""
        report = []
        report.append("PIPELINE EXECUTION REPORT")
        report.append("=" * 80)
        report.append(f"Timestamp: {self.start_time}")
        report.append(f"Status: {self.status}")
        report.append("")
        
        if self.end_time and self.start_time:
            start = datetime.strptime(self.start_time, '%Y-%m-%d %H:%M:%S')
            end = datetime.strptime(self.end_time, '%Y-%m-%d %H:%M:%S')
            duration = (end - start).total_seconds()
            report.append(f"Duration: {duration:.2f} seconds")
            report.append("")
        
        # Stage-by-stage summary
        report.append("EXECUTION LOG:")
        report.append("-" * 80)
        
        for entry in self.execution_log:
            status_symbol = '✓' if entry['status'] == 'SUCCESS' else '✗' if entry['status'] == 'FAIL' else '⟳'
            report.append(f"[{entry['timestamp']}] {status_symbol} {entry['stage']}: {entry['message']}")
            if entry['details']:
                report.append(f"    └─ {entry['details']}")
            report.append("")
        
        # Summary by stage
        report.append("STAGE SUMMARY:")
        report.append("-" * 80)
        
        stages = {
            'STAGE 1': 'LOAD - Load raw data',
            'STAGE 2': 'PROFILE - Data quality analysis',
            'STAGE 3': 'DETECT PII - Identify sensitive data',
            'STAGE 4': 'VALIDATE - Schema validation',
            'STAGE 5': 'CLEAN - Normalize and handle missing values',
            'STAGE 6': 'MASK - Protect PII'
        }
        
        for stage, description in stages.items():
            stage_logs = [e for e in self.execution_log if e['stage'] == stage]
            if stage_logs:
                last_status = stage_logs[-1]['status']
                status_symbol = '✓' if last_status == 'SUCCESS' else '✗'
                report.append(f"{status_symbol} {stage}: {description}")
        
        report.append("")
        
        # Outputs generated
        report.append("OUTPUTS GENERATED:")
        report.append("-" * 80)
        
        outputs = [
            ('Data Files', [
                '../data/customers_cleaned.csv',
                '../data/customers_masked.csv'
            ]),
            ('Reports', [
                '../reports/data_quality_report.txt',
                '../reports/pii_detection_report.txt',
                '../reports/validation_results.txt',
                '../reports/cleaning_log.txt',
                '../reports/masked_sample.txt',
                '../reports/pipeline_execution_report.txt'
            ])
        ]
        
        for category, files in outputs:
            report.append(f"\n{category}:")
            for file in files:
                exists = os.path.exists(file)
                symbol = '✓' if exists else '✗'
                report.append(f"  {symbol} {file}")
        
        report.append("")
        
        # Final summary
        report.append("FINAL SUMMARY:")
        report.append("-" * 80)
        
        if self.status == 'SUCCESS':
            report.append("✓ Pipeline completed successfully")
            report.append("")
            report.append("Data quality pipeline successfully processed customer data:")
            report.append("  1. Profiled raw data and identified quality issues")
            report.append("  2. Detected PII and assessed exposure risk")
            report.append("  3. Validated data against schema rules")
            report.append("  4. Cleaned data (normalized formats, handled missing values)")
            report.append("  5. Masked PII for privacy protection")
            report.append("")
            report.append("Output datasets ready for use:")
            report.append("  • customers_cleaned.csv - Clean data with business utility")
            report.append("  • customers_masked.csv - Privacy-safe data for analytics")
        else:
            report.append("✗ Pipeline encountered errors")
            report.append("")
            report.append("Review execution log above for details.")
        
        report.append("")
        
        return "\n".join(report)
    
    def run(self):
        """Execute the complete pipeline"""
        self.start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.status = 'RUNNING'
        
        print("=" * 80)
        print("STARTING DATA QUALITY & PII PROTECTION PIPELINE")
        print("=" * 80)
        print(f"Start time: {self.start_time}")
        print()
        
        try:
            # Stage 1: Load
            df_raw, error = self.stage_1_load()
            if error:
                self.status = 'FAILED'
                return False
            print()
            
            # Stage 2: Profile
            success, error = self.stage_2_profile(df_raw)
            if error:
                self.status = 'FAILED'
                return False
            print()
            
            # Stage 3: Detect PII
            success, error = self.stage_3_detect_pii(df_raw)
            if error:
                self.status = 'FAILED'
                return False
            print()
            
            # Stage 4: Validate
            validation_results, error = self.stage_4_validate(df_raw)
            if error:
                self.status = 'FAILED'
                return False
            print()
            
            # Stage 5: Clean
            df_cleaned, error = self.stage_5_clean(df_raw)
            if error:
                self.status = 'FAILED'
                return False
            print()
            
            # Stage 6: Mask PII
            df_masked, error = self.stage_6_mask_pii(df_cleaned)
            if error:
                self.status = 'FAILED'
                return False
            print()
            
            self.status = 'SUCCESS'
            
        except Exception as e:
            self.status = 'FAILED'
            self.log('PIPELINE', 'FAIL', f'Unexpected error: {str(e)}')
            traceback.print_exc()
            return False
        
        finally:
            self.end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Generate execution report
            report = self.generate_execution_report()
            
            # Save execution report
            report_path = os.path.join(self.reports_dir, 'pipeline_execution_report.txt')
            with open(report_path, 'w') as f:
                f.write(report)
            
            print()
            print("=" * 80)
            print(f"PIPELINE {self.status}")
            print("=" * 80)
            print(f"End time: {self.end_time}")
            print(f"Execution report saved to: {report_path}")
            print()
        
        return self.status == 'SUCCESS'

if __name__ == "__main__":
    # Initialize and run pipeline
    pipeline = DataPipeline(
        input_file='/home/jeffmint/PII/data/customers_raw.csv',
        output_dir='/data',
        reports_dir='/reports'
    )
    
    success = pipeline.run()
    
    if success:
        print("✓ All stages completed successfully!")
        print()
        print("Next steps:")
        print("  1. Review all reports in ../reports/")
        print("  2. Use customers_cleaned.csv for business operations")
        print("  3. Use customers_masked.csv for analytics/ML")
        print("  4. Document any remaining data quality issues")
    else:
        print("✗ Pipeline failed. Review logs for details.")
        exit(1)