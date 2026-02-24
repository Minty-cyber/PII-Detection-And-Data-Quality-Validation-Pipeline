data-quality:
	python3 /home/jeffmint/PII/src/data_quality_analysis.py

pii-detection:
	python3 /home/jeffmint/PII/src/pii_detection.py

validate-data:
	python3 /home/jeffmint/PII/src/data_validator.py

clean-data:
	python3 /home/jeffmint/PII/src/data_cleaner.py

mask-pii:
	python3 /home/jeffmint/PII/src/pii_masker.py

pipeline:
	python3 /home/jeffmint/PII/src/pipeline.py