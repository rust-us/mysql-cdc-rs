const $connect_subscribe = document.getElementById('connect_subscribe');
// 获取所有用于 ip 输入框的引用
const $ip_inputs = document.querySelectorAll('.ip-input-group-input input');
const $connect_status_info = document.querySelector('#connect_status_info')
const $log_area = document.querySelector('#log_area')

// /* 客户端主动提交信息 */
// const $chat_send_form = document.querySelector('#chat_send_form')

/** @type {WebSocket | null} */
let ws = null

/* 按钮点击事件*/
$connect_subscribe.addEventListener('click', () => {
    let rs = false;
    if (ws) {
        rs = disconnect()
    } else {
        rs = connect()
    }

    updateConnectionStatus()
})

// /* 客户端主动提交信息 */
// $chat_send_form.addEventListener('submit', (ev) => {
//     ev.preventDefault()
//
//     const chat_send_text = document.querySelector('#chat_send_text')
//
//     const text = chat_send_text.value
//     record_log('Sending: ' + text)
//     sendMessage(text)
//
//     chat_send_text.value = ''
//     chat_send_text.focus()
// })

// 追加 log_area
function record_log(msg, type = 'status') {
    $log_area.innerHTML += `<p class="msg msg--${type}">${msg}</p>`
    $log_area.scrollTop += 1000
    // document.getElementById('messages').textContent += ev.data + '\n';
}

// 清空 log_area
function record_log_clear() {
    $log_area.innerHTML = ''
}

/*
* return
*   false: connect fail
*   true:  connect success
*/
function connect() {
    const log_form_data = document.querySelector('#log_form_data')
    const formData = new FormData(log_form_data);
    const choose_type = formData.get('choose_type');
    const username = formData.get('username');
    const passwd = formData.get('passwd');
    if (choose_type === '-1') {
        swal("请选择订阅方式", "请选择订阅方式", "warning");
        return false;
    }
    const host = assembly_ip_address();
    console.log(`host: ${host}`);
    // if (host === '') {
    //     return false;
    // }

    disconnect()

    const { location } = window

    const proto = location.protocol.startsWith('https') ? 'wss' : 'ws'
    const wsUri = `${proto}://${location.host}/ws`

    record_log('Connecting...')
    ws = new WebSocket(wsUri)

    // 连接打开时触发
    ws.addEventListener('open', (event) => {
        // 创建一个空对象来存储转换后的数据
        let jsonData = {};

        // 将FormData的entries转换为数组
        const formDataEntries = Array.from(formData.entries());
        // 使用传统的for循环遍历这个数组
        for (let i = 0; i < formDataEntries.length; i++) {
            const [key, value] = formDataEntries[i]; // 解构赋值获取键和值
            console.log(`key: ${key}, value: ${value}`);

            // 检查value是否是字符串（这里简化了检查，实际可能需要更复杂的逻辑来处理其他类型）
            if (typeof value === 'string') {
                // 如果是字符串，则添加到jsonData对象中
                jsonData[key] = value;
            }
            // 注意：如果value是File或Blob，这里不会处理
        }
        jsonData['host'] = host;

        record_log('Connection opened ...')
        console.log('WebSocket connection opened:', event);

        // 将表单数据转换为JSON字符串（可选，取决于你的服务器如何处理数据）
        const post_data = createWSEvent(0, jsonData);
        sendMessage(JSON.stringify(post_data));

        updateConnectionStatus()
    });

    // 接收消息时触发
    ws.addEventListener('message', (ev) => {
        // 'Received from server: ' + ev.data, 'message'
        record_log(ev.data, 'message')
    });

    ws.onclose = function(event) {
        record_log('Disconnected' + event.reason)

        ws = null
        updateConnectionStatus()
    }

    ws.onerror = function(error) {
        console.error('WebSocket Error: ' + error);
    };

    return true;
}

// 发送消息
function sendMessage(msg) {
    ws.send(msg);
}

/*
* return
*   false: disconnect fail
*   true:  disconnect success
*/
function disconnect() {
    if (ws) {
        record_log('Disconnecting...')

        ws.close()
        ws = null

        updateConnectionStatus()

        return true;
    }

    return true;
}

function updateConnectionStatus() {
    if (ws) {
        // record_log_clear();

        $connect_status_info.style.backgroundColor = 'transparent'
        $connect_status_info.style.color = 'green'
        $connect_status_info.textContent = `connected`

        $connect_subscribe.classList.remove('btn-primary');
        $connect_subscribe.classList.add('btn-default');
        $connect_subscribe.innerHTML = '取消'
    } else {
        $connect_status_info.style.color = 'black'
        $connect_status_info.textContent = 'disconnected'

        $connect_subscribe.classList.remove('btn-default');
        $connect_subscribe.classList.add('btn-primary');
        $connect_subscribe.textContent = '订阅'
    }
}

/*
* 初始化IP地址字符串
*
* return ip address value, or empty('')
*/
function assembly_ip_address() {
    // 初始化IP地址字符串
    let ipAddress = '';
    let ver = true;

    $ip_inputs.forEach(function(input, index) {
        if (!ver) {
            return;
        }

        // 获取输入框的值，并去除前后空白
        const value = input.value.trim();

        // 验证输入值是否为数字（可选，根据你的需求来）
        if (!/^\d+$/.test(value)) {
            // swal("请输入有效的数字作为IP地址的一部分", "请输入有效的数字作为IP地址的一部分", "warning");
            ver = false; // 如果验证失败，可以提前退出
            return;
        }

        // 添加到IP地址字符串中
        // 如果不是第一个输入，则在前面加上点号
        if (index > 0) {
            ipAddress += '.';
        }
        ipAddress += value;
    });

    if (!ver) {
        return '';
    }

    return ipAddress;
}

// 定义事件对象
function createWSEvent(action, body) {
    return {
        action: action,
        body: body,
    };
}