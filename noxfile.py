import nox


@nox.session(python=["3.8", "3.9", "3.10", "3.11", "3.12"])
def lint(session):
    session.install("pytest")
    session.install("ruff")
    session.install(".")

    session.run("ruff check")
    session.run("pytest")