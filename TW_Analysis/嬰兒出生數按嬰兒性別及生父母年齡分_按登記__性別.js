var config = { responsive: true }

var 嬰兒出生數按嬰兒性別及生父母年齡分_按登記__性別 = {"data": [{"type": "scatter", "name": "\u7537", "x": ["110", "111", "112", "113"], "y": [79507, 72089, 70221, 69939], "mode": "lines"}, {"type": "scatter", "name": "\u5973", "x": ["110", "111", "112", "113"], "y": [74303, 66883, 65337, 64905], "mode": "lines"}], "layout": {"title": {"text": "\u5b30\u5152\u51fa\u751f\u6578\u6309\u5b30\u5152\u6027\u5225\u53ca\u751f\u7236\u6bcd\u5e74\u9f61\u5206_\u6309\u767b\u8a18__\u6027\u5225 110~113", "font": {"family": "Times New Roman"}, "x": 0.05, "y": 0.9}, "hovermode": "x", "xaxis": {"type": "category", "tickfont": {"family": "Courier New", "size": 14}, "automargin": true}, "font": {"family": "Courier New", "color": "#ffffff"}, "yaxis": {"tickfont": {"family": "Courier New"}, "automargin": true}, "plot_bgcolor": "#000", "paper_bgcolor": "#000"}};

Plotly.newPlot("嬰兒出生數按嬰兒性別及生父母年齡分_按登記__性別",
    嬰兒出生數按嬰兒性別及生父母年齡分_按登記__性別.data,
    嬰兒出生數按嬰兒性別及生父母年齡分_按登記__性別.layout || {},
    config);