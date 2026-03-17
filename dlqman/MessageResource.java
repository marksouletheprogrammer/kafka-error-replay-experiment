package se.irori.rest;

import io.smallrye.mutiny.Uni;
import se.irori.model.Message;
import se.irori.service.MessageService;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/v1/messages")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class MessageResource {

    @Inject
    MessageService messageService;

    @GET
    public Uni<List<Message>> listMessages() {
        return messageService.list();
    }
}
