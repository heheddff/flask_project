from wtforms.validators import ValidationError


class CustomValidators(object):

    # 全局验证器,字段是否为空
    @classmethod
    def not_empty(cls, message=None):
        if message is None:
            message = '字段不能留空'

        def is_empty(form, field):
            if len(field.data.strip()) == 0:
                raise ValidationError(message)

        return is_empty
