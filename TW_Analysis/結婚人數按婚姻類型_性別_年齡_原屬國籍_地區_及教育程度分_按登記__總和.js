var config = { responsive: true }

var 結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記__總和 = {"data": [{"type": "scatter", "name": "number_of_marry", "x": ["109", "110", "111", "112", "113"], "y": [243404, 229212, 249994, 250384, 246122], "mode": "lines"}], "layout": {"title": {"font": {"family": "Times New Roman"}, "x": 0.05, "y": 0.9, "text": "\u7d50\u5a5a\u4eba\u6578\u6309\u5a5a\u59fb\u985e\u578b_\u6027\u5225_\u5e74\u9f61_\u539f\u5c6c\u570b\u7c4d_\u5730\u5340_\u53ca\u6559\u80b2\u7a0b\u5ea6\u5206_\u6309\u767b\u8a18__\u7e3d\u548c 109~113"}, "font": {"family": "Courier New", "color": "#ffffff"}, "xaxis": {"tickfont": {"family": "Courier New", "size": 14}, "automargin": true, "type": "category"}, "yaxis": {"tickfont": {"family": "Courier New"}, "automargin": true}, "plot_bgcolor": "#000", "paper_bgcolor": "#000", "legend": {"font": {"color": "#ffffff"}}, "hovermode": "x"}};

Plotly.newPlot("結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記__總和",
    結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記__總和.data,
    結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記__總和.layout || {},
    config);