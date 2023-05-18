package cn.itcast.hotel.exception;

import cn.itcast.hotel.pojo.PageResult;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(Exception.class)
    public PageResult ex(Exception ex){
        ex.printStackTrace();
        return null;
    }
}
