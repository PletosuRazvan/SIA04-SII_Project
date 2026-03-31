import os, glob

base = r'd:\Facultate\SII\Proiect'

all_sql = glob.glob(os.path.join(base, '**', '*.sql'), recursive=True)
print(f'Found {len(all_sql)} SQL files\n')

total_removed = 0

for filepath in sorted(all_sql):
    rel = os.path.relpath(filepath, base)
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    original_lines = len(content.split('\n'))

    lines = content.split('\n')
    new_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('--'):
            continue
        if stripped and all(c in ' -=*#_|+/\\' for c in stripped):
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
    new_line_count = len(new_content.split('\n'))
    removed = original_lines - new_line_count

    if removed > 0:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f'  {rel}: {original_lines} -> {new_line_count} ({removed} removed)')
        total_removed += removed
    else:
        print(f'  {rel}: OK (no comments)')

print(f'\nTotal: {total_removed} comment lines removed from {len(all_sql)} files')
