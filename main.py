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

    return os, pd, sqlalchemy


@app.cell
def _(pd):
    loan_default = pd.read_csv('data/loan_default.csv')
    return (loan_default,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Connecting to our database""")
    return


@app.cell
def _(os, sqlalchemy):
    user = "postgres.rtkdpjkstgkdgfdxfssk"
    password = os.environ.get("POSTGRES_PASSWORD", "4Q5rGypzheEknaOP")
    DATABASE_URL = (
                f"postgresql://{user}:{password}"
                "@aws-0-eu-west-2.pooler.supabase.com:6543/postgres"
            )
    engine = sqlalchemy.create_engine(DATABASE_URL)
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
def _(loan_default, mo):
    _df = mo.sql(
        f"""
        SELECT COUNT(*) FROM loan_default
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##### 1.3 Showing the first 5 of data""")
    return


@app.cell
def _(loan_default, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM loan_default LIMIT 5
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##### 1.4 Selecting specific columns in our database""")
    return


@app.cell
def _(loan_default, mo):
    _df = mo.sql(
        f"""
        SELECT id, year, gender FROM loan_default
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##### 1.5 Getting unique values for categorical columns""")
    return


@app.cell
def _(loan_default, mo):
    _df = mo.sql(
        f"""
        SELECT DISTINCT gender FROM loan_default
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##### 1.5 FIltering data""")
    return


@app.cell
def _(loan_default, mo):
    _df = mo.sql(
        f"""
        SELECT * 
        FROM loan_default
        WHERE gender = 'Female'
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##### 1.6 Removing missing values in columns""")
    return


@app.cell
def _(loan_default, mo):
    _df = mo.sql(
        f"""
        SELECT id, loan_limit, loan_purpose, approv_in_adv
        FROM loan_default
        WHERE loan_limit IS NOT NULL
            AND loan_purpose IS NOT NULL
            AND approv_in_adv IS NOT NULL
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##### 1.7 Common Table Expressions (CTEs) / Sub Queries""")
    return


@app.cell
def _(loan_default, mo):
    _df = mo.sql(
        f"""
        WITH total_loan_types AS (
        SELECT 
          DISTINCT loan_type, 
          COUNT(*) AS total
        FROM loan_default
        GROUP BY loan_type
        ORDER BY loan_type, total ASC)

        SELECT * FROM total_loan_types
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""CTEs simply make life easier, providing an easy way to make subqueries""")
    return


@app.cell
def _(loan_default, mo):
    _df = mo.sql(
        f"""
        SELECT 
            DISTINCT id,
            year,
            CASE 
                WHEN gender = 'Sex Not Available' THEN NULL
                WHEN gender = 'Male' THEN 'M'
                WHEN gender = 'Female' THEN 'F'
                WHEN gender = 'Joint' THEN 'Couple'

            ELSE gender END AS gender,
            loan_type,
            approv_in_adv, 
            loan_purpose,
            credit_worthiness,
            loan_amount,
            lump_sum_payment,
            status
        FROM loan_default
        WHERE gender IS NOT NULL AND credit_worthiness = 'l1'
        """
    )
    return


@app.cell
def _(loan_default, mo):
    df = mo.sql(
        f"""
        WITH cleaned_data AS (
            SELECT 
            DISTINCT id,
            year,
            CASE 
                WHEN gender = 'Sex Not Available' THEN NULL
                WHEN gender = 'Male' THEN 'M'
                WHEN gender = 'Female' THEN 'F'
                WHEN gender = 'Joint' THEN 'Couple'

            ELSE gender END AS gender,
            loan_type,
            approv_in_adv, 
            loan_purpose,
            credit_worthiness,
            loan_amount,
            lump_sum_payment,
            status
        FROM loan_default
        WHERE credit_worthiness = 'l1')

        SELECT id,
            gender,
            loan_type,
            approv_in_adv, 
            loan_amount,
            lump_sum_payment,
            status
            FROM cleaned_data
        WHERE gender IS NOT NULL
            AND approv_in_adv IS NOT NULL
            AND loan_purpose IS NOT NULL 
        """
    )
    return (df,)


@app.cell
def _(df):
    df.to_csv("data/clean_loan_data.csv")
    return


if __name__ == "__main__":
    app.run()
