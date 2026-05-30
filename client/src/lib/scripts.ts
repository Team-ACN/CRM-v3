export type DbMode = 'new' | 'old'

export interface ScriptDef {
  id: string
  filename: string
  label: string
  caption: string
}

export interface SectionDef {
  title: string
  icon: string
  cols: 4 | 5
  scripts: ScriptDef[]
}

export const SECTIONS: SectionDef[] = [
  {
    title: 'Leads & Growth',
    icon: '🚀',
    cols: 4,
    scripts: [
      { id: 'all-leads',             filename: 'all-leads.py',             label: '👥 All Leads',       caption: 'All lead records → Sheet' },
      { id: 'enquires',              filename: 'enquires.py',              label: '❓ Enquiries',        caption: 'Buyer enquiry data → Sheet' },
      { id: 'requirement_enquiries', filename: 'requirement_enquiries.py', label: '🎯 Req. Enquiries',  caption: 'Requirement enquiries → Sheet' },
      { id: 'leads',                 filename: 'leads.py',                 label: '🔐 Tried Access',    caption: 'Unauthorised access attempts' },
    ],
  },
  {
    title: 'Inventory Management',
    icon: '🏢',
    cols: 4,
    scripts: [
      { id: 'inventories',     filename: 'inventories-from-firebase.py', label: '📦 Inventories',      caption: 'Main inventory sheet sync' },
      { id: 'new-inventory',   filename: 'new-inventory.py',             label: '🆕 New Inventory',    caption: 'New format inventory sheet' },
      { id: 'new-inventory-2', filename: 'new-inventory-2.py',           label: '🆕 Product Analysis', caption: 'Product analysis sheet' },
      { id: 'qc',              filename: 'QC.py',                        label: '🔍 QC Properties',    caption: 'QC-reviewed properties' },
    ],
  },
  {
    title: 'System & Data',
    icon: '⚙️',
    cols: 5,
    scripts: [
      { id: 'req',                  filename: 'req.py',                  label: '📋 Requirements', caption: 'Buyer requirement records' },
      { id: 'agents',               filename: 'agents.py',               label: '🛡️ Agents',       caption: 'Agent roster → Sheet' },
      { id: 'connecthistory',       filename: 'connecthistory.py',       label: '📞 Agents Calls', caption: 'Agent call history' },
      { id: 'connecthistory-leads', filename: 'connecthistory_leads.py', label: '📞 Leads Calls',  caption: 'Lead call history' },
      { id: 'truestate',            filename: 'truestate-sync.py',       label: '🔗 TrueState Apex', caption: 'Apex CRM sync' },
    ],
  },
]

export const SCRIPT_ALLOWLIST = new Set(
  SECTIONS.flatMap((s) => s.scripts.map((sc) => sc.filename))
)
