var config = { responsive: true }

var 嬰兒出生數按嬰兒性別及出生胎別分_按登記__總和 = {"data": [{"type": "scatter", "name": "\u5b30\u5152\u51fa\u751f\u6578", "x": [110, 111, 112, 113], "y": [153820, 138986, 135571, 134856], "mode": "lines"}], "layout": {"title": {"text": "\u5b30\u5152\u51fa\u751f\u6578\u6309\u5b30\u5152\u6027\u5225\u53ca\u51fa\u751f\u80ce\u5225\u5206_\u6309\u767b\u8a18__\u7e3d\u548c 110~113", "font": {"family": "Times New Roman"}, "x": 0.05, "y": 0.9}, "hovermode": "x", "xaxis": {"type": "category", "tickfont": {"family": "Courier New", "size": 14}, "automargin": true}, "font": {"family": "Courier New", "color": "#ffffff"}, "yaxis": {"tickfont": {"family": "Courier New"}, "automargin": true}, "plot_bgcolor": "#000", "paper_bgcolor": "#000"}};

Plotly.newPlot("嬰兒出生數按嬰兒性別及出生胎別分_按登記__總和",
    嬰兒出生數按嬰兒性別及出生胎別分_按登記__總和.data,
    嬰兒出生數按嬰兒性別及出生胎別分_按登記__總和.layout || {},
    config);