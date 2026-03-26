"""Checklists module models.

This module owns checklist templates and engagement-applied checklists.
"""

from __future__ import annotations

from sqlalchemy import Column, Date, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, Index
from sqlalchemy.orm import relationship

from db.base import Base


class ChecklistTemplate(Base):
    __tablename__ = "checklist_templates"

    template_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    status = Column(String, nullable=False, default="active")
    audience = Column(String, nullable=False, default="internal")  # 'internal' | 'user'
    created_at = Column(DateTime(timezone=True), nullable=False)
    created_employee_id = Column(Integer, ForeignKey("employee.employee_id", ondelete="SET NULL"), nullable=True)

    items = relationship(
        "ChecklistTemplateItem",
        back_populates="template",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="ChecklistTemplateItem.item_id.asc()",
    )


class ChecklistTemplateItem(Base):
    __tablename__ = "checklist_template_items"

    item_id = Column(Integer, primary_key=True)
    template_id = Column(Integer, ForeignKey("checklist_templates.template_id", ondelete="CASCADE"), nullable=False)
    title = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    display_order = Column(Integer, nullable=True)

    template = relationship("ChecklistTemplate", back_populates="items")


class EngagementChecklist(Base):
    __tablename__ = "engagement_checklists"

    checklist_id = Column(Integer, primary_key=True)
    engagement_id = Column(Integer, ForeignKey("engagements.engagement_id", ondelete="CASCADE"), nullable=False)
    template_id = Column(Integer, ForeignKey("checklist_templates.template_id"), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    created_employee_id = Column(Integer, ForeignKey("employee.employee_id", ondelete="SET NULL"), nullable=True)

    __table_args__ = (
        UniqueConstraint("engagement_id", "template_id", name="uq_engagement_checklists_engagement_template"),
        Index("ix_engagement_checklists_engagement_id", "engagement_id"),
    )

    template = relationship("ChecklistTemplate")
    tasks = relationship(
        "EngagementChecklistTask",
        back_populates="checklist",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="EngagementChecklistTask.task_id.asc()",
    )


class EngagementChecklistTask(Base):
    __tablename__ = "engagement_checklist_tasks"

    task_id = Column(Integer, primary_key=True)
    checklist_id = Column(
        Integer,
        ForeignKey("engagement_checklists.checklist_id", ondelete="CASCADE"),
        nullable=False,
    )
    item_id = Column(Integer, ForeignKey("checklist_template_items.item_id"), nullable=False)
    assigned_employee_id = Column(Integer, ForeignKey("employee.employee_id", ondelete="SET NULL"), nullable=True)
    status = Column(String, nullable=False, default="pending")
    notes = Column(Text, nullable=True)
    due_date = Column(Date, nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    completed_by_employee_id = Column(Integer, ForeignKey("employee.employee_id", ondelete="SET NULL"), nullable=True)

    __table_args__ = (
        Index("ix_engagement_checklist_tasks_checklist_id", "checklist_id"),
        Index("ix_engagement_checklist_tasks_assigned_employee_id", "assigned_employee_id"),
    )

    checklist = relationship("EngagementChecklist", back_populates="tasks")
    item = relationship("ChecklistTemplateItem")
