var config = { responsive: true }

var 結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記__性別 = {"data": [{"type": "scatter", "name": "\u7537", "x": ["109", "110", "111", "112", "113"], "y": [120663, 113820, 123890, 124097, 121904], "mode": "lines"}, {"type": "scatter", "name": "\u5973", "x": ["109", "110", "111", "112", "113"], "y": [122741, 115392, 126104, 126287, 124218], "mode": "lines"}], "layout": {"title": {"text": "\u7d50\u5a5a\u4eba\u6578\u6309\u5a5a\u59fb\u985e\u578b_\u6027\u5225_\u5e74\u9f61_\u539f\u5c6c\u570b\u7c4d_\u5730\u5340_\u53ca\u6559\u80b2\u7a0b\u5ea6\u5206_\u6309\u767b\u8a18__\u6027\u5225 109~113", "font": {"family": "Times New Roman"}, "x": 0.05, "y": 0.9}, "hovermode": "x", "xaxis": {"type": "category", "tickfont": {"family": "Courier New", "size": 14}, "automargin": true}, "font": {"family": "Courier New", "color": "#ffffff"}, "yaxis": {"tickfont": {"family": "Courier New"}, "automargin": true}, "plot_bgcolor": "#000", "paper_bgcolor": "#000"}};

Plotly.newPlot("結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記__性別",
    結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記__性別.data,
    結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記__性別.layout || {},
    config);