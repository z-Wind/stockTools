var config = { responsive: true }

var 嬰兒出生數按嬰兒性別及生父母年齡分_按登記__總和 = {"data": [{"type": "scatter", "name": "birth_count", "x": ["110", "111", "112", "113"], "y": [153820, 138986, 135571, 134856], "mode": "lines"}], "layout": {"title": {"font": {"family": "Times New Roman"}, "x": 0.05, "y": 0.9, "text": "\u5b30\u5152\u51fa\u751f\u6578\u6309\u5b30\u5152\u6027\u5225\u53ca\u751f\u7236\u6bcd\u5e74\u9f61\u5206_\u6309\u767b\u8a18__\u7e3d\u548c 110~113"}, "font": {"family": "Courier New", "color": "#ffffff"}, "xaxis": {"tickfont": {"family": "Courier New", "size": 14}, "automargin": true, "type": "category"}, "yaxis": {"tickfont": {"family": "Courier New"}, "automargin": true}, "plot_bgcolor": "#000", "paper_bgcolor": "#000", "legend": {"font": {"color": "#ffffff"}}, "hovermode": "x"}};

Plotly.newPlot("嬰兒出生數按嬰兒性別及生父母年齡分_按登記__總和",
    嬰兒出生數按嬰兒性別及生父母年齡分_按登記__總和.data,
    嬰兒出生數按嬰兒性別及生父母年齡分_按登記__總和.layout || {},
    config);