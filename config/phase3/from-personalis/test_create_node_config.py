#!/usr/bin/env python3


pattern_tests = {
    "phase3_fastq" : "va_mvp_phase3/DVALABP001234/SHIP1234567/HMY5FDSX5_SHIP1234567_ACGGAACA-GTTCTCGT_L001_R1_001.fastq.gz",
    "phase3_personalis" : "va_mvp_phase3/DVALABP001234/SHIP1234567/SHIP1234567.json",
    "phase3_checksum" : "va_mvp_phase3/DVALABP001234/SHIP1234567/checksum.txt",
    "phase2_fastq" : "va_mvp_phase2/DVALABP123456/SHIP1234567/FASTQ/SHIP1234567_0_R1.fastq.gz",
    "phase2_gtc" : "va_mvp_phase2/DVALABP123456/SHIP1234567/Microarray/SHIP1234567_microarray.gtc",
    "phase2_idat" : "va_mvp_phase2/DVALABP123456/SHIP1234567/Microarray/SHIP1234567_microarray_Grn.idat",
    "phase2_personalis" : "va_mvp_phase2/DVALABP123456/SHIP1234567/SHIP1234567.json",
    "phase2_checksum" : "va_mvp_phase2/DVALABP123456/SHIP1234567/checksum.txt"
}

match_patterns = {
   "Blob": [r"^va_mvp_phase\d\/(?P<plate>\w+)\/(?P<sample>\w+)\/.*"],
   "Fastq": [
             r"^va_mvp_phase2\/.*\/.*\/FASTQ\/.*\.fastq\.gz$",
             r"^va_mvp_phase3\/\w+\/\w+\/(?P<machine>[a-zA-Z0-9]+)_[a-zA-Z0-9]+_(?P<primer1>[ACGTU]+)-(?P<primer2>[ACGTU]+)_L(?P<lane>[0-9]+)_R(?P<matepair>\d)_(?P<unknown>\d+)\.fastq\.gz"], 
   "Microarray": ["^va_mvp_phase2/.*/.*/Microarray/.*"], 
   "PersonalisSequencing": ["^va_mvp_phase\\d/.*\\.json$"],
   "Json": ["^va_mvp_phase\\d/.*\\.json$"],
   "Checksum": ["^va_mvp_phase\\d/.*checksum.txt"], 
   "WGS35": ["^va_mvp_phase\\d/.*"],
   "FromPersonalis": ["^va_mvp_phase\\d/.*"],
}

chatgpt_prompt = """There is a dictionary named "match_patterns" where each key is a label and each value is a list of regular expression patterns. Create a dictionary called "pattern_matches" and populate it with key-value pairs. The keys should be a copy of the keys in pattern_tests and each value should be an empty list. For each value in a dictionary named "pattern_tests", check to see whether the value is a full match for any of the regular expressions in match_patterns. If any of the patterns match, update the pattern_matches dictionary entry for the corresponding pattern tests key. Update the value of that entry to include they key of the match_pattern regular expression that matched the pattern_tests value."""

import re

#match_patterns = {"label1": [r"pattern1", r"pattern2"], "label2": [r"pattern3"]}
#pattern_tests = {"key1": "test1", "key2": "test2", "key3": "test3"}

pattern_matches = {key:	 [] for key in pattern_tests.keys()}

for test_key, test_value in pattern_tests.items():
    for label, patterns in match_patterns.items():
        for pattern in patterns:
            if re.fullmatch(pattern, test_value):
                pattern_matches[test_key].append(label)

for test_input, labels in pattern_matches.items():
    print(f"{test_input}: {labels}")
