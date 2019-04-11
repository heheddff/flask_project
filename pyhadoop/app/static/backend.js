$("span.cursor").click(function() {
    ser_id= $(this).parent().attr('id')
    stype= $(this).attr('data')
    ajaxPost(ser_id,stype)

});

//全选

$("#allSelect").click(function () {
    $("input[type='checkbox']").each(function () {
        if ($(this).attr('disabled') != 'disabled'){
            $(this).attr('checked', true)
        }
    });
})

$("#reserveSelect").click(function () {
    $("input[type='checkbox']").each(function () {
        if ($(this).attr('disabled') != 'disabled'){
            $(this).attr('checked', false)
        }
    });
})

$("a.btn").click(function () {
    inputs = $("input:checked")
    stype= $(this).attr('data')
    ser_ids = ''
    if (inputs.length > 0){
        inputs.each(function () {
            ser_id= $(this).parent().attr('id')
            span = $(this).parent().find('span')
            layout = $("#zhe")
            ser_ids += ser_id + ','
        })
        ajaxPost(ser_ids,stype)
    }else{
        alert('please choose server!')
    }
})


function ajaxPost(ser_id,stype){
    result = {1:'已入库',2:'未入库',3:'异常',4:'Error'}
    cs = {1:'text-muted',2:'text-success',3:'text-danger',4:'text-danger'}
    waitinfo = {1:'正在入库...',2:'正在关闭...',3:'正在重新入库...'}
    data = { serid:ser_id, stype:stype}
    layout = $("#zhe")
    $.ajax({
        //增加遮罩层，防止多次提交
        beforeSend: function () {
            layout.removeClass('hidden')
            layout.addClass('layout')
        },
        error: function (jqXHR, textStatus, errorThrown) {
            layout.removeClass('layout')
            console.log(jqXHR)
            console.log(textStatus)
            console.log(errorThrown)
        },
        data: data,
        dataType: "json",
        method: "post",
        global:false,
        crossDomain: true,
        success: function (res, textStatus) {
            results = res.result
            for (i in results){
                status = results[i]
                ii = i.split('.')
                lis = $("#"+ii[0]+'\\.'+ii[1])
                lis.attr('class',cs[status])

                span = lis.find('span')
                span.removeClass('cursor')
                span.text(result[status])

                span.prev().attr('disabled', 'disabled') //input add disabled
                span.prev().attr('checked', false) //input add disabled
                span.off()
            }
            layout.removeClass('layout').addClass('hidden') //div remove layout class
        },
        url: './add'
    })
}

