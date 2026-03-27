/**
 * 侧边栏导航点击 → 滚动到目标图表
 * 使用 capture=true 在 React/Dash 拦截之前处理点击
 */
document.addEventListener('click', function(e) {
    var link = e.target.closest('a.nav-item');
    if (!link) return;

    var href = link.getAttribute('href');
    if (!href || href.charAt(0) !== '#') return;

    var targetId = href.substring(1);
    var target = document.getElementById(targetId);
    if (!target) return;

    e.preventDefault();
    e.stopImmediatePropagation();
    target.scrollIntoView({ behavior: 'smooth', block: 'start' });

    var orig = target.style.outline;
    target.style.outline = '2px solid #FFD700';
    setTimeout(function() { target.style.outline = orig; }, 1500);
}, true);
