"""
Reusable HTML -> PDF component (WeasyPrint + Jinja2).

Renders a Jinja2 template to HTML and converts it to a PDF, returning the raw
bytes so callers can attach/upload them directly (e.g. via
``utils.clsZohoMailing.ZohoMailer`` or ``utils.ftp.FTPConn``).

The component is deliberately report-agnostic: give it a template directory and
a context, get PDF bytes back. It ships with a couple of broadly useful Jinja
filters (Spanish/Catalan ``number_format`` and a ``data_uri`` helper for
embedding local images/fonts) and lets callers register extra filters.

Usage:
    from utils.clsPdf import PdfBuilder

    pdf = PdfBuilder(template_dir="/opt/airflow/dags/templates/kpi")
    data = pdf.render("report.html.j2", {"title": "Hola", ...})
    # data is `bytes` -> attach or write to disk

Native libraries required in the image (Debian/Bookworm):
    libpango-1.0-0 libpangoft2-1.0-0 libcairo2 libgdk-pixbuf-2.0-0
    libffi-dev shared-mime-info fonts (optional)
"""

from __future__ import annotations

import base64
import mimetypes
from pathlib import Path
from typing import Callable, Optional


def number_format(value, decimals: int = 2, dec_point: str = ",", thousands_sep: str = ".") -> str:
    """PHP-style ``number_format``: fixed decimals, custom separators.

    Mirrors ``number_format($v, $decimals, ',', '.')`` used in the original
    Blade templates so ported reports produce identical strings.
    """
    try:
        v = float(value)
    except (TypeError, ValueError):
        v = 0.0
    negative = v < 0
    s = f"{abs(v):,.{decimals}f}"          # 1,234,567.89  (en_US grouping)
    int_part, _, frac_part = s.partition(".")
    int_part = int_part.replace(",", thousands_sep)
    out = int_part + (dec_point + frac_part if decimals > 0 else "")
    return ("-" + out) if negative and v != 0 else out


def data_uri(path: str) -> str:
    """Return a ``data:`` URI for a local file (for CSP-free embedding)."""
    p = Path(path)
    mime, _ = mimetypes.guess_type(str(p))
    mime = mime or "application/octet-stream"
    b64 = base64.b64encode(p.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{b64}"


class PdfBuilder:
    """Render Jinja2 templates to PDF bytes via WeasyPrint."""

    def __init__(
        self,
        template_dir: str,
        base_url: Optional[str] = None,
        extra_filters: Optional[dict[str, Callable]] = None,
    ):
        from jinja2 import Environment, FileSystemLoader, select_autoescape

        self.template_dir = str(template_dir)
        # base_url lets WeasyPrint resolve any relative url()/src in the HTML
        # (fonts, images). Defaults to the template dir.
        self.base_url = base_url or self.template_dir

        self.env = Environment(
            loader=FileSystemLoader(self.template_dir),
            autoescape=select_autoescape(["html", "xml", "j2"]),
        )
        self.env.filters["number_format"] = number_format
        self.env.filters["data_uri"] = data_uri
        if extra_filters:
            self.env.filters.update(extra_filters)

    def render_html(self, template_name: str, context: dict) -> str:
        """Render a template to an HTML string."""
        return self.env.get_template(template_name).render(**(context or {}))

    def render(self, template_name: str, context: dict) -> bytes:
        """Render a template and return the resulting PDF as bytes."""
        from weasyprint import HTML

        html = self.render_html(template_name, context)
        return HTML(string=html, base_url=self.base_url).write_pdf()

    def render_string(self, html: str) -> bytes:
        """Convert an already-rendered HTML string to PDF bytes."""
        from weasyprint import HTML

        return HTML(string=html, base_url=self.base_url).write_pdf()
