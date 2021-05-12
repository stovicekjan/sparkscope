/*
Set of functions used for setting time filters with buttons
*/

function dateString(date) {
    y = date.getFullYear();
    m = date.getMonth() + 1;
    d = date.getDate();
    h = date.getHours();
    M = date.getMinutes();

    dateStr = y + '-' + (m <= 9 ? '0' + m : m) + '-' + (d <= 9 ? '0' + d : d);
    timeStr = (h <= 9 ? '0' + h : h) + ':' + (M <= 9 ? '0' + M : M);
    return dateStr + 'T' + timeStr;
}

function setDateToNow(elementId) {
    document.getElementById(elementId).value = dateString(new Date());
}

function setDateToMidnight(elementId) {
    var today_midnight = new Date();
    today_midnight.setHours(0,0,0,0);
    document.getElementById(elementId).value = dateString(today_midnight);
}

function setDateToWeekAgo(elementId) {
    var now = new Date();
    var date = new Date(now.setDate(now.getDate() - 7));
    document.getElementById(elementId).value = dateString(date);
}

function setDateToMonthAgo(elementId) {
    var now = new Date();
    var date = new Date(now.setMonth(now.getMonth() - 1));
    document.getElementById(elementId).value = dateString(date);
}

function setDateToLongAgo(elementId) {
    document.getElementById(elementId).value = dateString(new Date(2000, 0, 1));
}

document.getElementById('btn-start-today').onclick = function() {
    setDateToMidnight('start-from');
    setDateToNow('start-to');
}

document.getElementById('btn-start-7d').onclick = function() {
    setDateToWeekAgo('start-from');
    setDateToNow('start-to');
}

document.getElementById('btn-start-month').onclick = function() {
    setDateToMonthAgo('start-from');
    setDateToNow('start-to');
}

document.getElementById('btn-start-any').onclick = function() {
    setDateToLongAgo('start-from');
    setDateToNow('start-to');
}

document.getElementById('btn-end-today').onclick = function() {
    setDateToMidnight('end-from');
    setDateToNow('end-to');
}

document.getElementById('btn-end-7d').onclick = function() {
    setDateToWeekAgo('end-from');
    setDateToNow('end-to');
}

document.getElementById('btn-end-month').onclick = function() {
    setDateToMonthAgo('end-from');
    setDateToNow('end-to');
}

document.getElementById('btn-end-any').onclick = function() {
    setDateToLongAgo('end-from');
    setDateToNow('end-to');
}