import java.sql.Timestamp
import java.time.LocalDateTime

case class UserStat(timestamp: String, MainRoom: Double, AC: Double, LivingRoom: Double, LightsLiving: Double,
                    BathPlugs: Double, Holiday: Double, GuestRoom: Double, Fridge: Double, Kitchn: Double){

  /*val time: Timestamp = Timestamp.valueOf(LocalDateTime.now())
  //timestamp = time;*/
  override def toString = s"UserStat($timestamp, $MainRoom, $AC, $LivingRoom," +
    s" $LightsLiving, $BathPlugs, $Holiday, $GuestRoom, $Fridge, $Kitchn)"
}
