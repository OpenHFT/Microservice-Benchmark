package services.openmicro.driver.chronicle;

import services.openmicro.driver.api.EventHandler;

public interface ChronicleEventHandler extends EventHandler<ChronicleEvent> {
    @Override
    void event(ChronicleEvent event) ;
}
