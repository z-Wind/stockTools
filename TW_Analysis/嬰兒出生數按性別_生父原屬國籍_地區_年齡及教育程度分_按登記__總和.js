var config = { responsive: true }

var 嬰兒出生數按性別_生父原屬國籍_地區_年齡及教育程度分_按登記__總和 = {"data": [{"type": "scatter", "name": "\u5b30\u5152\u51fa\u751f\u6578", "x": [107, 108, 109, 110, 111, 112], "y": [181601, 177767, 165249, 153820, 138986, 135571], "mode": "lines"}], "layout": {"height": 600, "margin": {"b": 100}, "title": {"font": {"family": "Times New Roman"}, "x": 0.05, "y": 0.9, "text": "\u5b30\u5152\u51fa\u751f\u6578\u6309\u6027\u5225_\u751f\u7236\u539f\u5c6c\u570b\u7c4d_\u5730\u5340_\u5e74\u9f61\u53ca\u6559\u80b2\u7a0b\u5ea6\u5206_\u6309\u767b\u8a18__\u7e3d\u548c 107~112"}, "font": {"family": "Courier New", "color": "#ffffff"}, "xaxis": {"tickfont": {"family": "Courier New", "size": 14}, "automargin": true, "type": "category"}, "yaxis": {"tickfont": {"family": "Courier New"}, "automargin": true}, "plot_bgcolor": "#000", "paper_bgcolor": "#000", "legend": {"font": {"color": "#ffffff"}}, "hovermode": "x"}};

Plotly.newPlot("嬰兒出生數按性別_生父原屬國籍_地區_年齡及教育程度分_按登記__總和",
    嬰兒出生數按性別_生父原屬國籍_地區_年齡及教育程度分_按登記__總和.data,
    嬰兒出生數按性別_生父原屬國籍_地區_年齡及教育程度分_按登記__總和.layout || {},
    config);