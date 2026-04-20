"""Generate Finnish OmaVero PDF attachments from lot-level gains data.

Two PDFs per year:
  luovutusvoitot_{year}.pdf   -- profitable lots (gain >= 0)
  luovutustappiot_{year}.pdf  -- loss-making lots (gain < 0)
"""

import datetime
import os
import shutil
import subprocess
import tempfile

import click
import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fi(value):
    """Finnish locale number: 1~234,56 (LaTeX ~ = non-breaking space)."""
    s = f"{abs(value):,.2f}".replace(",", "~").replace(".", ",")
    return f"-{s}" if value < 0 else s


def _fmt_date(ts):
    if pd.isna(ts):
        return ""
    return pd.Timestamp(ts).strftime("%d.%m.%Y")


def _fmt_amount(value):
    """Crypto amount: up to 8 decimal places, trailing zeros stripped."""
    return f"{float(value):.8f}".rstrip("0").rstrip(".")


def _collect_lots(pairs, year):
    frames = []
    for pair in pairs:
        book = pair.xc.book.copy()
        if book.empty:
            continue
        book["selldate"] = pd.to_datetime(book["selldate"])
        book["buydate"]  = pd.to_datetime(book["buydate"])
        book["buyvalue"]  = book["buyvalue"].astype(float)
        book["sellvalue"] = book["sellvalue"].astype(float)
        yb = book[book["selldate"].dt.year == year].copy()
        if yb.empty:
            continue
        yb.insert(0, "asset", pair.asset)
        frames.append(yb)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    combined["gain"] = combined["sellvalue"] - combined["buyvalue"]
    return combined


# ---------------------------------------------------------------------------
# LaTeX generation
# ---------------------------------------------------------------------------

_COL_HEADER = (
    r"\textbf{Omaisuus} & \textbf{Hankintapäivä} & \textbf{Myyntipäivä} & "
    r"\textbf{Määrä} & \textbf{Hankintameno (\texteuro)} & "
    r"\textbf{Luovutushinta (\texteuro)} & \textbf{Voitto/tappio (\texteuro)} \\"
)


def _table_rows(rows):
    lines = []
    for _, row in rows.iterrows():
        asset    = str(row["asset"]).replace("_", r"\_").replace("&", r"\&")
        buydate  = _fmt_date(row["buydate"])
        selldate = _fmt_date(row["selldate"])
        amount   = _fmt_amount(row["amount"])
        lines.append(
            f"  {asset} & {buydate} & {selldate} & {amount} & "
            f"{_fi(row['buyvalue'])} & {_fi(row['sellvalue'])} & {_fi(row['gain'])} \\\\"
        )

    total_buy  = rows["buyvalue"].sum()
    total_sell = rows["sellvalue"].sum()
    total_gain = rows["gain"].sum()
    lines.append(r"  \midrule")
    lines.append(
        r"  \textbf{Yhteensä} & & & & "
        + rf"\textbf{{{_fi(total_buy)}}} & "
        + rf"\textbf{{{_fi(total_sell)}}} & "
        + rf"\textbf{{{_fi(total_gain)}}} \\"
    )
    return "\n".join(lines)


def _build_latex(rows, year, title):
    body = _table_rows(rows)
    return f"""\
\\documentclass[a4paper,10pt]{{article}}
\\usepackage[utf8]{{inputenc}}
\\usepackage[T1]{{fontenc}}
\\usepackage{{textcomp}}
\\usepackage{{booktabs}}
\\usepackage{{longtable}}
\\usepackage[a4paper,landscape,margin=1.5cm]{{geometry}}

\\title{{{title} {year}}}
\\date{{Laadittu {datetime.date.today().strftime("%d.%m.%Y")}}}
\\author{{}}
\\begin{{document}}
\\maketitle

\\begin{{longtable}}{{llllrrr}}
\\toprule
{_COL_HEADER}
\\midrule
\\endfirsthead
\\multicolumn{{7}}{{l}}{{\\textit{{(jatkoa)}}}}\\\\
\\midrule
{_COL_HEADER}
\\midrule
\\endhead
\\midrule
\\multicolumn{{7}}{{r}}{{\\textit{{(jatkuu seuraavalla sivulla)}}}}\\\\
\\endfoot
\\bottomrule
\\endlastfoot
{body}
\\end{{longtable}}

\\vspace{{1em}}
\\noindent Osto- ja myyntipalkkiot on sisällytetty hankintamenoon ja luovutushintaan muissa kuin niissä tapauksissa, joissa on käytetty hankintameno-olettamaa.

\\end{{document}}
"""


# ---------------------------------------------------------------------------
# Compilation
# ---------------------------------------------------------------------------

def _compile(tex_src, output_path):
    with tempfile.TemporaryDirectory() as tmp:
        tex_file = os.path.join(tmp, "report.tex")
        with open(tex_file, "w", encoding="utf-8") as f:
            f.write(tex_src)
        result = None
        for _ in range(2):
            result = subprocess.run(
                ["pdflatex", "-interaction=nonstopmode", "report.tex"],
                cwd=tmp, capture_output=True,
            )
        pdf_src = os.path.join(tmp, "report.pdf")
        if not os.path.exists(pdf_src):
            log = result.stdout.decode(errors="replace")
            raise RuntimeError(f"pdflatex failed:\n{log[-3000:]}")
        shutil.copy2(pdf_src, output_path)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate_pdfs(pairs, year, pdf_dir):
    os.makedirs(pdf_dir, exist_ok=True)
    combined = _collect_lots(pairs, year)
    if combined.empty:
        click.echo(f"No sells in {year} — no PDFs written.", err=True)
        return

    specs = [
        (
            combined[combined["gain"] >= 0],
            f"luovutusvoitot_{year}.pdf",
            "Kryptovara -- luovutusvoittolaskelma",
        ),
        (
            combined[combined["gain"] < 0],
            f"luovutustappiot_{year}.pdf",
            "Kryptovara -- luovutustappiolaskelma",
        ),
    ]
    for rows, filename, title in specs:
        if rows.empty:
            click.echo(
                f"No {'profits' if 'voitot' in filename else 'losses'} in {year}"
                f" — skipping {filename}.",
                err=True,
            )
            continue
        tex = _build_latex(rows, year, title)
        out = os.path.join(pdf_dir, filename)
        click.echo(f"Compiling {filename}…", err=True)
        _compile(tex, out)
        click.echo(f"Wrote {out}", err=True)
