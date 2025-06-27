import marimo

__generated_with = "0.14.7"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Basic PostGresSQL and project""")
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import os
    import sys
    import sqlalchemy
    import pandas as pd

    return pd, sqlalchemy


@app.cell
def _(pd):
    loan_default = pd.read_csv('data/loan_default.csv')
    return (loan_default,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Connecting to our database""")
    return


@app.cell
def _(sqlalchemy):
    db = ""
    user = ""
    password = ""
    port = ""

    DATABASE_URL = (
                f"postgresql://{user}:{password}"
                f"@aws-0-eu-west-2.pooler.supabase.com:{port}/{db}"
            )
    engine = sqlalchemy.create_engine(DATABASE_URL)
    print(DATABASE_URL)
    return (engine,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##### 1.1 Querying a table in our database""")
    return


@app.cell
def _(engine, loan_default, mo, public):
    _df = mo.sql(
        f"""
        SELECT * FROM public.loan_default
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##### 1.2 Finding out how much data we have""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##### 1.3 Showing the first 5 of data""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##### 1.4 Selecting specific columns in our database""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##### 1.5 Getting unique values for categorical columns""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##### 1.5 FIltering data""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##### 1.6 Removing missing values in columns""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##### 1.7 Common Table Expressions (CTEs) / Sub Queries""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""CTEs simply make life easier, providing an easy way to make subqueries""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""

        """
    )
    return


@app.cell
def _(mo):
    df = mo.sql(
        f"""

        """
    )
    return (df,)


@app.cell
def _(df):
    df.to_csv("data/clean_loan_data.csv")
    return


if __name__ == "__main__":
    app.run()
