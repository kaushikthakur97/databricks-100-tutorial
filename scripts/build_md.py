"""Convert all Databricks .py notebooks to .md files in docs/."""
import os, glob

NOTEBOOKS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "notebooks")
DOCS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs")

def convert_py_to_md(filepath, outpath):
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    start = 1 if lines and lines[0].startswith("# Databricks notebook source") else 0
    state = "md"
    output = []
    first_code = True

    for line in lines[start:]:
        stripped = line.rstrip('\n')

        if stripped.strip() == "# COMMAND ----------":
            if state == "code":
                output.append("```\n\n")
            state = "code"
            output.append("```python\n")
            continue

        if stripped.startswith("# MAGIC %md"):
            if state == "code":
                output.append("```\n\n")
            state = "md"
            continue

        if stripped.startswith("# MAGIC "):
            if state == "code":
                output.append("```\n\n")
                state = "md"
            output.append(stripped[7:] + '\n')
            continue

        if stripped.strip() == "# MAGIC":
            output.append('\n')
            continue

        output.append(stripped + '\n')

    if state == "code":
        output.append("```\n")

    with open(outpath, 'w', encoding='utf-8') as f:
        f.writelines(output)

for nbfile in sorted(glob.glob(os.path.join(NOTEBOOKS_DIR, "*.py"))):
    name = os.path.basename(nbfile)
    outfile = os.path.join(DOCS_DIR, name.replace('.py', '.md'))
    print(f"Converting: {name} -> {os.path.basename(outfile)}")
    convert_py_to_md(nbfile, outfile)

print("Done! All markdown docs regenerated.")
