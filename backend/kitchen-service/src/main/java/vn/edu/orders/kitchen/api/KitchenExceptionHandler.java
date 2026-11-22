package vn.edu.orders.kitchen.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import vn.edu.orders.kitchen.application.BadKitchenRequestException;
import vn.edu.orders.kitchen.application.TicketConflictException;
import vn.edu.orders.kitchen.application.TicketNotFoundException;

@RestControllerAdvice
public class KitchenExceptionHandler {

    @ExceptionHandler(TicketConflictException.class)
    ProblemDetail handleConflict(TicketConflictException ex) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
        pd.setTitle("Invalid ticket status transition");
        pd.setProperty("orderId", ex.orderId());
        pd.setProperty("currentStatus", ex.currentStatus());
        pd.setProperty("expectedStatus", ex.expectedStatus());
        return pd;
    }

    @ExceptionHandler(TicketNotFoundException.class)
    ProblemDetail handleNotFound(TicketNotFoundException ex) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        pd.setTitle("Kitchen ticket not found");
        pd.setProperty("orderId", ex.orderId());
        return pd;
    }

    @ExceptionHandler(BadKitchenRequestException.class)
    ProblemDetail handleBadRequest(BadKitchenRequestException ex) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
        pd.setTitle("Invalid kitchen request");
        return pd;
    }
}
