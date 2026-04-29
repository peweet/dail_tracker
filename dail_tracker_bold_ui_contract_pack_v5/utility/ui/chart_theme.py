from __future__ import annotations


def apply_civic_plotly_theme(fig, *, height: int = 360, showlegend: bool = True):
    """Apply a restrained editorial theme to a Plotly figure.

    This helper must not calculate metrics or mutate source data.
    """
    fig.update_layout(
        height=height,
        margin=dict(l=16, r=16, t=32, b=32),
        font=dict(family="system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"),
        paper_bgcolor="white",
        plot_bgcolor="white",
        hovermode="x unified",
        showlegend=showlegend,
    )
    fig.update_xaxes(showgrid=False, zeroline=False, title=None)
    fig.update_yaxes(showgrid=True, zeroline=False, title=None)
    return fig


def civic_altair_config(chart):
    """Apply a minimal editorial Altair config."""
    return chart.configure_view(
        strokeWidth=0
    ).configure_axis(
        labelFont="system-ui",
        titleFont="system-ui",
        gridOpacity=0.25
    ).configure_legend(
        labelFont="system-ui",
        titleFont="system-ui"
    )
