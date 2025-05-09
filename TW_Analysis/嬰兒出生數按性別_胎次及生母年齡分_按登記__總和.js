var config = { responsive: true }

var 嬰兒出生數按性別_胎次及生母年齡分_按登記__總和 = {"data": [{"type": "scatter", "name": "\u5b30\u5152\u51fa\u751f\u6578", "x": [106, 107, 108, 109, 110, 111, 112, 113], "y": [193844, 181601, 177767, 165249, 153820, 138986, 135571, 134856], "mode": "lines"}], "layout": {"title": {"text": "\u5b30\u5152\u51fa\u751f\u6578\u6309\u6027\u5225_\u80ce\u6b21\u53ca\u751f\u6bcd\u5e74\u9f61\u5206_\u6309\u767b\u8a18__\u7e3d\u548c 106~113", "font": {"family": "Times New Roman"}, "x": 0.05, "y": 0.9}, "hovermode": "x", "xaxis": {"type": "category", "tickfont": {"family": "Courier New", "size": 14}, "automargin": true}, "font": {"family": "Courier New", "color": "#ffffff"}, "yaxis": {"tickfont": {"family": "Courier New"}, "automargin": true}, "plot_bgcolor": "#000", "paper_bgcolor": "#000"}};

Plotly.newPlot("嬰兒出生數按性別_胎次及生母年齡分_按登記__總和",
    嬰兒出生數按性別_胎次及生母年齡分_按登記__總和.data,
    嬰兒出生數按性別_胎次及生母年齡分_按登記__總和.layout || {},
    config);