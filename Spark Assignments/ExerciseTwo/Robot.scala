class Robot(robot_ID: String, robotType: String, mobility: String, universalUID: String) extends Serializable {
  override def toString: String = {
    s"$robot_ID , $robotType , $mobility , $universalUID"
  }
}
