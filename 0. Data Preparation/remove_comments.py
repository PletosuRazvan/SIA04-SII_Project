import os, glob

base = r'd:\Facultate\SII\Proiect'

cloud_sql = glob.glob(os.path.join(base, '**', 'CLOUD_*.sql'), recursive=True)
py_files = glob.glob(os.path.join(base, '0. Data Preparation', '*.py'))
pg_sql = [
    os.path.join(base, '1. Data Sources', f) 
    for f in ['00.SCHEMA_pg.sql','01.staging_pg.sql','03.create_pg.sql','04.insert_pg.sql']
]

all_files = sorted(set(cloud_sql + py_files + pg_sql))
print(f'Processing {len(all_files)} files...')

for filepath in all_files:
    rel = os.path.relpath(filepath, base)
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    original_lines = len(content.split('\n'))

    if filepath.endswith('.sql'):
        lines = content.split('\n')
        new_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith('--'):
                continue
            if stripped and all(c in ' -=*#_|+/' for c in stripped):
                continue
            new_lines.append(line)
        final_lines = []
        prev_blank = False
        for line in new_lines:
            if line.strip() == '':
                if not prev_blank:
                    final_lines.append(line)
                prev_blank = True
            else:
                final_lines.append(line)
                prev_blank = False
        new_content = '\n'.join(final_lines).strip() + '\n'

    elif filepath.endswith('.py'):
        lines = content.split('\n')
        new_lines = []
        in_docstring = False
        for line in lines:
            stripped = line.strip()
            if stripped.startswith('#!'):
                new_lines.append(line)
                continue
            if '"""' in stripped:
                new_lines.append(line)
                count = stripped.count('"""')
                if count == 1:
                    in_docstring = not in_docstring
                continue
            if in_docstring:
                new_lines.append(line)
                continue
            if stripped.startswith('#'):
                continue
            new_lines.append(line)
        final_lines = []
        prev_blank = False
        for line in new_lines:
            if line.strip() == '':
                if not prev_blank:
                    final_lines.append(line)
                prev_blank = True
            else:
                final_lines.append(line)
                prev_blank = False
        new_content = '\n'.join(final_lines).strip() + '\n'
    else:
        continue

    new_line_count = len(new_content.split('\n'))
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
    removed = original_lines - new_line_count
    print(f'  {rel}: {original_lines} -> {new_line_count} ({removed} removed)')

print('\nDone!')
