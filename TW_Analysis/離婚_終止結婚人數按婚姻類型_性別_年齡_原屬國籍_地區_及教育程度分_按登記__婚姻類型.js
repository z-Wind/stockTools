var config = { responsive: true }

var 離婚_終止結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記__婚姻類型 = {"data": [{"type": "scatter", "name": "\u4e0d\u540c\u6027\u5225", "x": ["111", "112", "113"], "y": [99994, 104608, 105086], "mode": "lines"}, {"type": "scatter", "name": "\u76f8\u540c\u6027\u5225", "x": ["111", "112", "113"], "y": [1224, 1562, 1852], "mode": "lines"}], "layout": {"title": {"text": "\u96e2\u5a5a_\u7d42\u6b62\u7d50\u5a5a\u4eba\u6578\u6309\u5a5a\u59fb\u985e\u578b_\u6027\u5225_\u5e74\u9f61_\u539f\u5c6c\u570b\u7c4d_\u5730\u5340_\u53ca\u6559\u80b2\u7a0b\u5ea6\u5206_\u6309\u767b\u8a18__\u5a5a\u59fb\u985e\u578b 111~113", "font": {"family": "Times New Roman"}, "x": 0.05, "y": 0.9}, "hovermode": "x", "xaxis": {"type": "category", "tickfont": {"family": "Courier New", "size": 14}, "automargin": true}, "font": {"family": "Courier New", "color": "#ffffff"}, "yaxis": {"tickfont": {"family": "Courier New"}, "automargin": true}, "plot_bgcolor": "#000", "paper_bgcolor": "#000"}};

Plotly.newPlot("離婚_終止結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記__婚姻類型",
    離婚_終止結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記__婚姻類型.data,
    離婚_終止結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記__婚姻類型.layout || {},
    config);