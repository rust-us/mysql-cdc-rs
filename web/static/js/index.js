// ip输入器组件(继承bootstrap的样式)
$('[data-plugin="ipinput"]').each(function(){
    var elem = $(this);
    var inputs = elem.find('input');
    var len = inputs.length;
    var letter_limit = parseInt(elem.attr('data-letterlimit'));
    if(!letter_limit){ letter_limit = 3; }
    elem.find('input').each(function(index, item){
        var $item = $(item);
        $item.on('focus', function(){
            if($(this).val()!=''){
                $(this).select();
            }
            elem.addClass('focus');
            $(this).addClass('focus');
        });
        $item.on('blur', function(){
            elem.removeClass('focus');
            $(this).removeClass('focus');
        });
        $item.on('keyup', function(e){
            var thisinput = $(this);
            var v = $(this).val();
            // 输入↓或→键自动跳到下一个输入框内
            if( (e.keyCode == 39 || e.keyCode == 40) && index<len-1){
                inputs.eq(index+1).focus();
            }
            // 输入↓或→键自动跳到上一个输入框内
            else if( (e.keyCode == 38 || e.keyCode == 37) && index!=0){
                inputs.eq(index-1).focus();
            }
            // 输入3个字符自动跳到下一个输入框内
            else if(v.length == letter_limit && index<len-1){
                inputs.eq(index+1).focus();
            }
            // 删除的时候，一个输入框没有了字符，自动跳回上一个输入框
            else if(v == '' && e.keyCode == 8 && index!=0){
                inputs.eq(index-1).focus();
            }
        });
    })
});

