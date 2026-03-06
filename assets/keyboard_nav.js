/**
 * 键盘左右箭头平移图表，Shift加速
 */
document.addEventListener('keydown', function(e) {
    if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
    // 不拦截输入框中的按键
    var tag = document.activeElement && document.activeElement.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;

    var graphs = document.querySelectorAll('.js-plotly-plot');
    var step = e.shiftKey ? 120 : 30;  // Shift加速4倍
    var shift = (e.key === 'ArrowLeft') ? -step : step;

    graphs.forEach(function(gd) {
        if (!gd.layout || !gd.layout.xaxis) return;
        var range = gd.layout.xaxis.range;
        if (!range || range.length < 2) return;

        var newRange = [range[0] + shift, range[1] + shift];
        Plotly.relayout(gd, {'xaxis.range': newRange});
    });

    e.preventDefault();
});
