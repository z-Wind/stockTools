var config = { responsive: true }

var 嬰兒出生數按性別_生母原屬國籍_地區_年齡及教育程度分_按登記__性別 = {"data": [{"type": "scatter", "name": "\u7537", "x": ["106", "107", "108", "109", "110", "111", "112", "113"], "y": [100464, 93865, 92221, 85695, 79499, 72087, 70217, 69933], "mode": "lines"}, {"type": "scatter", "name": "\u5973", "x": ["106", "107", "108", "109", "110", "111", "112", "113"], "y": [93360, 87721, 85522, 79523, 74287, 66868, 65322, 64894], "mode": "lines"}], "layout": {"title": {"text": "\u5b30\u5152\u51fa\u751f\u6578\u6309\u6027\u5225_\u751f\u6bcd\u539f\u5c6c\u570b\u7c4d_\u5730\u5340_\u5e74\u9f61\u53ca\u6559\u80b2\u7a0b\u5ea6\u5206_\u6309\u767b\u8a18__\u6027\u5225 106~113", "font": {"family": "Times New Roman"}, "x": 0.05, "y": 0.9}, "hovermode": "x", "xaxis": {"type": "category", "tickfont": {"family": "Courier New", "size": 14}, "automargin": true}, "font": {"family": "Courier New", "color": "#ffffff"}, "yaxis": {"tickfont": {"family": "Courier New"}, "automargin": true}, "plot_bgcolor": "#000", "paper_bgcolor": "#000"}};

Plotly.newPlot("嬰兒出生數按性別_生母原屬國籍_地區_年齡及教育程度分_按登記__性別",
    嬰兒出生數按性別_生母原屬國籍_地區_年齡及教育程度分_按登記__性別.data,
    嬰兒出生數按性別_生母原屬國籍_地區_年齡及教育程度分_按登記__性別.layout || {},
    config);