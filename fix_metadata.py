#!/usr/bin/env python3
import re

def fix_metadata(content):
    """Fix arrow.Metadata format in the Go file."""
    
    # Pattern to match metadata blocks
    metadata_pattern = r'&arrow\.Metadata\{Keys: \[\]string\{([^}]+)\}'
    
    def transform_metadata(match):
        metadata_content = match.group(1)
        lines = metadata_content.strip().split('\n')
        
        keys = []
        values = []
        
        for line in lines:
            line = line.strip()
            if line.endswith(','):
                line = line[:-1]
            if ':' in line:
                # Extract key and value from lines like '"description": "Ledger sequence number (primary key)"'
                parts = line.split(':', 1)
                key = parts[0].strip().strip('"')
                value = parts[1].strip().strip('"')
                keys.append(f'"{key}"')
                values.append(f'"{value}"')
        
        # Format the output
        if keys:
            keys_str = ', '.join(keys)
            values_str = ', '.join(values)
            return f'&arrow.Metadata{{Keys: []string{{{keys_str}}}, Values: []string{{{values_str}}}}'
        else:
            return '&arrow.Metadata{}'
    
    # Apply transformation
    result = re.sub(metadata_pattern, transform_metadata, content, flags=re.MULTILINE | re.DOTALL)
    
    return result

# Read the file
with open('/home/tillman/Documents/obsrvr-stellar-components/schemas/stellar_schemas.go', 'r') as f:
    content = f.read()

# Fix the metadata
fixed_content = fix_metadata(content)

# Write back
with open('/home/tillman/Documents/obsrvr-stellar-components/schemas/stellar_schemas.go', 'w') as f:
    f.write(fixed_content)

print("Fixed metadata format in stellar_schemas.go")