var config = { responsive: true }

var 嬰兒出生數按性別_胎次及生母年齡分_按登記__性別 = {"data": [{"type": "scatter", "name": "\u7537", "x": [106, 107, 108, 109, 110, 111, 112, 113], "y": [100477, 93876, 92237, 85704, 79513, 72097, 70227, 69947], "mode": "lines"}, {"type": "scatter", "name": "\u5973", "x": [106, 107, 108, 109, 110, 111, 112, 113], "y": [93367, 87725, 85530, 79545, 74307, 66889, 65344, 64909], "mode": "lines"}], "layout": {"height": 600, "margin": {"b": 100}, "title": {"font": {"family": "Times New Roman"}, "x": 0.05, "y": 0.9, "text": "\u5b30\u5152\u51fa\u751f\u6578\u6309\u6027\u5225_\u80ce\u6b21\u53ca\u751f\u6bcd\u5e74\u9f61\u5206_\u6309\u767b\u8a18__\u6027\u5225 106~113"}, "font": {"family": "Courier New", "color": "#ffffff"}, "xaxis": {"tickfont": {"family": "Courier New", "size": 14}, "automargin": true, "type": "category"}, "yaxis": {"tickfont": {"family": "Courier New"}, "automargin": true}, "plot_bgcolor": "#000", "paper_bgcolor": "#000", "legend": {"font": {"color": "#ffffff"}}, "hovermode": "x"}};

Plotly.newPlot("嬰兒出生數按性別_胎次及生母年齡分_按登記__性別",
    嬰兒出生數按性別_胎次及生母年齡分_按登記__性別.data,
    嬰兒出生數按性別_胎次及生母年齡分_按登記__性別.layout || {},
    config);